package cacheimport

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/moby/buildkit/solver"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestSimpleMarshal(t *testing.T) {
	cc := NewCacheChains()

	addRecords := func() {
		foo := cc.Add(outputKey(dgst("foo"), 0))
		bar := cc.Add(outputKey(dgst("bar"), 1))
		baz := cc.Add(outputKey(dgst("baz"), 0))

		baz.LinkFrom(foo, 0, "")
		baz.LinkFrom(bar, 1, "sel0")
		r0 := &solver.Remote{
			Descriptors: []ocispec.Descriptor{{
				Digest: dgst("d0"),
			}, {
				Digest: dgst("d1"),
			}},
		}
		baz.AddResult(time.Now(), r0)
	}

	addRecords()

	cfg, _, err := cc.Marshal()
	require.NoError(t, err)

	require.Equal(t, len(cfg.Layers), 2)
	require.Equal(t, len(cfg.Records), 3)

	require.Equal(t, cfg.Layers[0].Blob, dgst("d0"))
	require.Equal(t, cfg.Layers[0].ParentIndex, -1)
	require.Equal(t, cfg.Layers[1].Blob, dgst("d1"))
	require.Equal(t, cfg.Layers[1].ParentIndex, 0)

	require.Equal(t, cfg.Records[0].Digest, outputKey(dgst("baz"), 0))
	require.Equal(t, len(cfg.Records[0].Inputs), 2)
	require.Equal(t, len(cfg.Records[0].Results), 1)

	require.Equal(t, cfg.Records[1].Digest, outputKey(dgst("foo"), 0))
	require.Equal(t, len(cfg.Records[1].Inputs), 0)
	require.Equal(t, len(cfg.Records[1].Results), 0)

	require.Equal(t, cfg.Records[2].Digest, outputKey(dgst("bar"), 1))
	require.Equal(t, len(cfg.Records[2].Inputs), 0)
	require.Equal(t, len(cfg.Records[2].Results), 0)

	require.Equal(t, cfg.Records[0].Results[0].LayerIndex, 1)
	require.Equal(t, cfg.Records[0].Inputs[0][0].Selector, "")
	require.Equal(t, cfg.Records[0].Inputs[0][0].LinkIndex, 1)
	require.Equal(t, cfg.Records[0].Inputs[1][0].Selector, "sel0")
	require.Equal(t, cfg.Records[0].Inputs[1][0].LinkIndex, 2)

	// adding same info again doesn't produce anything extra
	addRecords()

	cfg2, descPairs, err := cc.Marshal()
	require.NoError(t, err)

	require.EqualValues(t, cfg, cfg2)

	// marshal roundtrip
	dt, err := json.Marshal(cfg)
	require.NoError(t, err)

	newChains := NewCacheChains()
	err = Parse(dt, descPairs, newChains)
	require.NoError(t, err)

	cfg3, _, err := cc.Marshal()
	require.NoError(t, err)
	require.EqualValues(t, cfg, cfg3)

	// add extra item
	cc.Add(outputKey(dgst("bay"), 0))
	cfg, _, err = cc.Marshal()
	require.NoError(t, err)

	require.Equal(t, len(cfg.Layers), 2)
	require.Equal(t, len(cfg.Records), 4)
}

func TestCrash(t *testing.T) {
	cc := NewCacheChains()

	addRecords := func() {
		l1 := cc.Add(digest.NewDigestFromEncoded("sha256", "c4139b59be98e8e2609be78c0750e4cfefada423d804c5d8a56d6153eead921f"))

		l2 := cc.Add(digest.NewDigestFromEncoded("sha256", "0b7ec3a160616f50903923a3793cb906a4f6111d1a919121fba0a2210be5b00c"))
		l1.LinkFrom(l2, 0, "")

		l3 := cc.Add(digest.NewDigestFromEncoded("sha256", "c21e017216cd820bcc0b649ad46c06dbaff39efb72bbff6ccced5bf563be8cbe"))
		l2.LinkFrom(l3, 0, "")

		l4 := cc.Add(digest.NewDigestFromEncoded("sha256", "b3a3f6ce36e85d288552de26dd4898660306f65dcf9be97e17fe8c76956afe1f"))
		l3.LinkFrom(l4, 0, "")

		l5 := cc.Add(digest.NewDigestFromEncoded("sha256", "2e012d1cd1f444050806d561b2a11cccd6418db63f1a1350f3fadb7cada951ec"))
		l4.LinkFrom(l5, 0, "")

		l6 := cc.Add(digest.NewDigestFromEncoded("sha256", "2fbdf0c9b333507d3116a87da12d16f8e5016df4647dd736af5b0fd1e8f30539"))
		l5.LinkFrom(l6, 0, "")

		l7 := cc.Add(digest.NewDigestFromEncoded("sha256", "e887de21c739414a4da9fb275d90a19eb2ebe273b6d6389a69c2f2c92e37c203"))
		l6.LinkFrom(l7, 0, "")

		l8 := cc.Add(digest.NewDigestFromEncoded("sha256", "7cb552dc6edecdaefcb58af9f731bcb3605653339cbfd64912376973fe5d36bc"))
		l7.LinkFrom(l8, 0, "")

		l9 := cc.Add(digest.NewDigestFromEncoded("sha256", "5208a5dd93aad59471413b7f61d430dead8ae614c3c34d88ad116eecfe5edbae"))
		l8.LinkFrom(l9, 0, "")

		l10 := cc.Add(digest.NewDigestFromEncoded("sha256", "f516c4a0e6b66b6b1a64ca32e48ffe3524180962aa11b6aaaf36fb96e5789700"))
		l9.LinkFrom(l10, 0, "")

		l11 := cc.Add(digest.NewDigestFromEncoded("sha256", "1301b63b26098c557366e1bf87d11b9a2c3d3d94181a1598459a65090b7e9d4b"))
		l10.LinkFrom(l11, 0, "")

		l12 := cc.Add(digest.NewDigestFromEncoded("sha256", "af1916b7122ad4a7b9aed15119790c782144a29bfb35c1ca6df26e11e86af6a5"))
		l11.LinkFrom(l12, 0, "")

		l13 := cc.Add(digest.NewDigestFromEncoded("sha256", "c066f738c0e14a6d3141cd0895f3ebb666ab00c6fd05d2456a64b9776ec6e997"))
		l12.LinkFrom(l13, 0, "")

		l14 := cc.Add(digest.NewDigestFromEncoded("sha256", "921fc680ac46d0f4b635bf07f9698432819fafc55e12618addcdda33bc6bfd57"))
		l13.LinkFrom(l14, 0, "")

		l15 := cc.Add(digest.NewDigestFromEncoded("sha256", "503bfe52c4b4249f6d74f64617dc1aee8849f15714f53499705bfa3bdefc3239"))
		l14.LinkFrom(l15, 0, "")

		l16 := cc.Add(digest.NewDigestFromEncoded("sha256", "807b3e8722c68572736f54d83fd741a5b1d69ccbef218fae52b4149a56d232ce"))
		l15.LinkFrom(l16, 0, "")

		l17 := cc.Add(digest.NewDigestFromEncoded("sha256", "d95ebf21c8c74b1d63b9931337c08efc399e429f817b4c7c65b5170f19ec8f7a"))
		l16.LinkFrom(l17, 0, "")

		l17b := cc.Add(digest.NewDigestFromEncoded("sha256", "a733984e29aa8448fa9a8d681e5900f14613e5a5055dad73911ca3191e284fa7"))
		l16.LinkFrom(l17b, 0, "")

		// baz.LinkFrom(foo, 0, "")
		// baz.LinkFrom(bar, 1, "sel0")
		r0 := &solver.Remote{
			Descriptors: []ocispec.Descriptor{{
				Digest: dgst("d0"),
			}, {
				Digest: dgst("d1"),
			}},
		}
		l1.AddResult(time.Now(), r0)
	}

	addRecords()

	_, _, err := cc.Marshal()
	require.Error(t, err)
}

func dgst(s string) digest.Digest {
	return digest.FromBytes([]byte(s))
}
