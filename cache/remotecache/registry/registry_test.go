package registry

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/containerd/containerd/remotes/docker"
	"github.com/moby/buildkit/cache/remotecache"
	"github.com/moby/buildkit/session"
	"github.com/moby/buildkit/util/resolver"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
)

/*
* Reproducer for https://github.com/moby/buildkit/issues/2088
 */
func TestResolvCacheExportFuncDelay(t *testing.T) {
	sm, err := session.NewManager()
	assert.NoError(t, err)

	startDummyGrpcServer(t, sm, "test-session")
	dummyRegistryServer := startDummyRegistryServer(t)

	imageRef := fmt.Sprintf("%s/library/ubuntu:latest", dummyRegistryServer)
	g := session.NewGroup("test-session")

	// controllerSolveCacheExporterCall represents the portion of code in Controller.Solve() which is
	// negatively affected by concurrent registry operations:
	// https://github.com/moby/buildkit/blob/921b0de92ecb9276952a8ed0dd7646aad9a786c6/control/control.go#L258
	controllerSolveCacheExporterCall := func() (remotecache.Exporter, error) {
		fn := ResolveCacheExporterFunc(sm, fakeHosts(dummyRegistryServer))
		s := map[string]string{attrRef: imageRef}
		return fn(context.Background(), g, s)
	}

	// start a registry interaction against our slow registry
	go func() {
		remote := resolver.DefaultPool.GetResolver(fakeHosts(dummyRegistryServer), imageRef, "push", sm, g)
		_, _, err := remote.Resolve(context.Background(), imageRef)
		assert.Error(t, err)
	}()

	// wait for registry interaction to hold the lock
	timerToBeSureConcurrentOperationHasLock := time.NewTimer(500 * time.Millisecond)
	<-timerToBeSureConcurrentOperationHasLock.C

	// now call the code that is affected by the lock being held
	start := time.Now()
	_, err = controllerSolveCacheExporterCall()
	assert.NoError(t, err)

	// if controllerSolveCacheExporterCall takes more than 3 seconds to run, a Controller.Status()
	// command launched at the same time would have failed by now with "no such job"
	if time.Since(start) > 3*time.Second {
		t.Fatal("acquire exporter func took too long")
	}
}

// startDummyRegistryServer starts a fake Docker registry that has the particularity to take
// 4 seconds to return tokens
func startDummyRegistryServer(t *testing.T) string {
	dummyRegistryServer := ""
	resp := func(res http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/v2/library/ubuntu/manifests/latest" && len(req.Header["Authorization"]) == 0 {
			res.Header().Add("Content-Type", "application/json; charset=utf-8")
			res.Header().Add("Docker-Distribution-Api-Version", "registry/2.0")
			res.Header().Add("Www-Authenticate", fmt.Sprintf(`Bearer realm="http://%s/token",service="registry.docker.io",scope="repository:library/ubuntu:pull"`, dummyRegistryServer))
			res.WriteHeader(401)
			res.Write([]byte(`{"errors":[{"code":"UNAUTHORIZED","message":"authentication required","detail":[{"Type":"repository","Class":"","Name":"maxlaverse/library/ubuntu","Action":"pull"}]}]}`))
		} else if req.URL.Path == "/token" {
			res.WriteHeader(200)
			res.Write([]byte(`{"token": "a-fake-token", "expires_in": 20,"issued_at": "2009-11-10T23:00:00Z"}`))
			slowDownTimer := time.NewTimer(time.Duration(4) * time.Second)
			<-slowDownTimer.C
		} else {
			res.WriteHeader(500)
			res.Write([]byte("Not implemented"))
		}
	}
	startDummyRegistryServer := httptest.NewServer(http.HandlerFunc(resp))
	dummyRegistryServer = startDummyRegistryServer.Listener.Addr().String()
	return dummyRegistryServer
}

func startDummyGrpcServer(t *testing.T, sm *session.Manager, testSessionName string) {
	grpcServer := grpc.NewServer()
	server, client := net.Pipe()
	go func() {
		(&http2.Server{}).ServeConn(server, &http2.ServeConnOpts{Handler: grpcServer})
	}()
	go func() {
		err := sm.HandleConn(context.Background(), client, map[string][]string{"X-Docker-Expose-Session-Uuid": {testSessionName}})
		assert.NoError(t, err)
	}()
}

func fakeHosts(addr string) docker.RegistryHosts {
	return func(t string) ([]docker.RegistryHost, error) {
		h := docker.RegistryHost{
			Scheme:       "http",
			Client:       &http.Client{},
			Host:         addr,
			Path:         "/v2",
			Capabilities: docker.HostCapabilityPull | docker.HostCapabilityResolve,
		}
		return []docker.RegistryHost{h}, nil
	}
}
