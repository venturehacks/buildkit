package remotecache

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/images"
	v1 "github.com/moby/buildkit/cache/remotecache/v1"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/util/compression"
	"github.com/moby/buildkit/util/contentutil"
	"github.com/moby/buildkit/util/progress"
	"github.com/moby/buildkit/util/progress/logs"
	digest "github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/image-spec/specs-go"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ResolveCacheExporterFunc func(ctx context.Context, g session.Group, attrs map[string]string) (Exporter, error)

func oneOffProgress(ctx context.Context, id string) func(err error) error {
	pw, _, _ := progress.NewFromContext(ctx)
	now := time.Now()
	st := progress.Status{
		Started: &now,
	}
	pw.Write(id, st)
	return func(err error) error {
		now := time.Now()
		st.Completed = &now
		pw.Write(id, st)
		pw.Close()
		return err
	}
}

type ExporterRef interface {
	Ref() string
}

type Exporter interface {
	solver.CacheExporterTarget
	// Finalize finalizes and return metadata that are returned to the client
	// e.g. ExporterResponseManifestDesc
	Finalize(ctx context.Context) (map[string]string, error)
}

const (
	// ExportResponseManifestDesc is a key for the map returned from Exporter.Finalize.
	// The map value is a JSON string of an OCI desciptor of a manifest.
	ExporterResponseManifestDesc = "cache.manifest"
)

type contentCacheExporter struct {
	solver.CacheExporterTarget
	chains             *v1.CacheChains
	ingester           content.Ingester
	oci                bool
	ref                string
	layerExportTimeout time.Duration
}

func NewExporter(ingester content.Ingester, ref string, oci bool) Exporter {
	layerExportTimeoutMinute := os.Getenv("LAYER_EXPORT_TIMEOUT_MINUTE")
	layerExportTimeoutMinuteInt, err := strconv.Atoi(layerExportTimeoutMinute)
	if err != nil {
		layerExportTimeoutMinuteInt = 5
	}

	cc := v1.NewCacheChains()
	logrus.Infof("remotecache.NewExporter() new CacheChains %p for %s (timeout: %d minutes)", cc, ref, layerExportTimeoutMinuteInt)
	return &contentCacheExporter{CacheExporterTarget: cc, chains: cc, ingester: ingester, oci: oci, ref: ref, layerExportTimeout: time.Minute * time.Duration(layerExportTimeoutMinuteInt)}
}

func (ce *contentCacheExporter) Ref() string {
	return fmt.Sprintf("remotecache:%s", ce.ref)
}

func (ce *contentCacheExporter) Finalize(ctx context.Context) (map[string]string, error) {
	res := make(map[string]string)
	marshalingDone := oneOffProgress(ctx, fmt.Sprintf("marshalling %s", ce.ref))
	config, descs, err := ce.chains.Marshal()
	marshalingDone(err)

	logrus.Infof("contentCacheExporter.Finalize() after Marshal: %s", ce.ref)
	if err != nil {
		return nil, err
	}

	// own type because oci type can't be pushed and docker type doesn't have annotations
	type manifestList struct {
		specs.Versioned

		MediaType string `json:"mediaType,omitempty"`

		// Manifests references platform specific manifests.
		Manifests []ocispec.Descriptor `json:"manifests"`
	}

	var mfst manifestList
	mfst.SchemaVersion = 2
	mfst.MediaType = images.MediaTypeDockerSchema2ManifestList
	if ce.oci {
		mfst.MediaType = ocispec.MediaTypeImageIndex
	}

	for _, l := range config.Layers {
		dgstPair, ok := descs[l.Blob]
		if !ok {
			return nil, errors.Errorf("missing blob %s", l.Blob)
		}
		layerDone := oneOffProgress(ctx, fmt.Sprintf("writing layer %s", l.Blob))

		err := func() error {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, ce.layerExportTimeout)
			defer cancel()

			if err := contentutil.Copy(ctxWithTimeout, ce.ingester, dgstPair.Provider, dgstPair.Descriptor, ce.ref, logs.LoggerFromContext(ctx)); err != nil {
				return layerDone(errors.Wrap(err, "error writing layer blob"))
			}
			return nil
		}()
		if err != nil {
			return nil, err
		}
		layerDone(nil)
		mfst.Manifests = append(mfst.Manifests, dgstPair.Descriptor)
	}

	mfst.Manifests = compression.ConvertAllLayerMediaTypes(ce.oci, mfst.Manifests...)

	dt, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	dgst := digest.FromBytes(dt)
	desc := ocispec.Descriptor{
		Digest:    dgst,
		Size:      int64(len(dt)),
		MediaType: v1.CacheConfigMediaTypeV0,
	}
	configDone := oneOffProgress(ctx, fmt.Sprintf("writing config %s", dgst))
	if err := content.WriteBlob(ctx, ce.ingester, dgst.String(), bytes.NewReader(dt), desc); err != nil {
		return nil, configDone(errors.Wrap(err, "error writing config blob"))
	}
	configDone(nil)

	mfst.Manifests = append(mfst.Manifests, desc)

	dt, err = json.Marshal(mfst)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal manifest")
	}
	dgst = digest.FromBytes(dt)

	desc = ocispec.Descriptor{
		Digest:    dgst,
		Size:      int64(len(dt)),
		MediaType: mfst.MediaType,
	}
	mfstDone := oneOffProgress(ctx, fmt.Sprintf("writing manifest %s", dgst))
	if err := content.WriteBlob(ctx, ce.ingester, dgst.String(), bytes.NewReader(dt), desc); err != nil {
		return nil, mfstDone(errors.Wrap(err, "error writing manifest blob"))
	}
	descJSON, err := json.Marshal(desc)
	if err != nil {
		return nil, err
	}
	res[ExporterResponseManifestDesc] = string(descJSON)
	mfstDone(nil)
	return res, nil
}
