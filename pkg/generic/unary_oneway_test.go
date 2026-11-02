package generic

import (
	"context"
	"testing"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

type unaryGenericNotify struct{ calls int }

func (h *unaryGenericNotify) GenericCall(ctx context.Context, method string, request any) (any, error) {
	h.calls++
	return "ignored-oneway-result", nil
}

func TestUnaryGenericOnewayCallsV1AndV2WithoutAResultObject(t *testing.T) {
	v1 := new(unaryGenericNotify)
	calls := 0
	v2 := &ServiceV2{GenericCall: func(context.Context, string, string, any) (any, error) { calls++; return "ignored-oneway-result", nil }}
	ri := rpcinfo.NewRPCInfo(rpcinfo.EmptyEndpointInfo(), rpcinfo.EmptyEndpointInfo(), rpcinfo.NewInvocation("Metadata", "Notify"), rpcinfo.NewRPCConfig(), rpcinfo.NewRPCStats())
	ctx := rpcinfo.NewCtxWithRPCInfo(context.Background(), ri)
	for _, handler := range []any{v1, v2} {
		if err := callHandler(ctx, handler, &Args{Method: "Notify", Request: map[string]any{"Name": "oneway"}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if v1.calls != 1 || calls != 1 {
		t.Fatal("oneway handler was skipped", v1.calls, calls)
	}
}
