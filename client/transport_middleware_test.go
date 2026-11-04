package client

import (
	"context"
	"reflect"
	"testing"

	internalclient "github.com/cloudwego/kitex/internal/client"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/remote"
)

// An installed transport alone must preserve the normal middleware chain.
// Explicitly disabling one component must not remove user instance middleware.
func TestTransportAndMiddlewareConfigurationAreIndependent(t *testing.T) {
	for _, custom := range []bool{false, true} {
		for _, disable := range []bool{false, true} {
			builds := 0
			o := internalclient.NewOptions(nil)
			o.IMWBs = []endpoint.MiddlewareBuilder{func(context.Context) endpoint.Middleware {
				builds++
				return func(next endpoint.Endpoint) endpoint.Endpoint { return next }
			}}
			o.MiddlewareConfig = MiddlewareConfig{DisableServiceDiscovery: true, DisableRPCTimeout: disable}
			if custom {
				o.UnaryTransport = &middlewareTestTransport{}
			}
			client := &kClient{opt: o}
			middleware := client.initMiddlewares(context.Background())
			if builds != 1 {
				t.Fatalf("transport=%v timeoutDisabled=%v: instance middleware builds=%d", custom, disable, builds)
			}
			wantUnary := 2 // service circuit breaker and default RPC timeout
			if disable {
				wantUnary--
			}
			if len(middleware.uMws) != wantUnary {
				t.Fatalf("transport=%v timeoutDisabled=%v: unary middleware=%d, want %d", custom, disable, len(middleware.uMws), wantUnary)
			}
			for _, close := range o.CloseCallbacks {
				_ = close()
			}
		}
	}
}

type middlewareTestTransport struct{ remote.UnaryTransport }

func TestMiddlewareConfigOptionPreservesExplicitSelection(t *testing.T) {
	config := MiddlewareConfig{DisableInstanceCircuitBreaker: true, DisableXDSRouter: true}
	o := internalclient.NewOptions([]internalclient.Option{WithMiddlewareConfig(config)})
	if !reflect.DeepEqual(o.MiddlewareConfig, config) {
		t.Fatal(o.MiddlewareConfig)
	}
}
