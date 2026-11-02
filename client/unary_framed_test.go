package client_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/kitex/client"
	kt "github.com/cloudwego/kitex/internal/mocks/thrift"
	"github.com/cloudwego/kitex/internal/mocks/thrift/testservice"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/remote"
	thriftcodec "github.com/cloudwego/kitex/pkg/remote/codec/thrift"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/cloudwego/kitex/transport"
)

type framedPayload struct {
	remote.PayloadCodec
	reads atomic.Int32
	fail  atomic.Bool
}

func (p *framedPayload) Unmarshal(ctx context.Context, msg remote.Message, in remote.ByteBuffer) error {
	p.reads.Add(1)
	if err := p.PayloadCodec.Unmarshal(ctx, msg, in); err != nil {
		return err
	}
	if p.fail.Load() {
		return errors.New("injected payload decode failure")
	}
	return nil
}

type framedHooks struct {
	unaryHooks
	header   atomic.Int32
	response func(context.Context, *remote.UnaryCall) error
}

func (h *framedHooks) HeaderBeforePayload() bool { return true }
func (h *framedHooks) ServerHeaderReady(context.Context, *remote.UnaryCall) error {
	h.header.Add(1)
	return errors.New("Framed admitted before Base decoding")
}

func (h *framedHooks) ClientResponse(ctx context.Context, c *remote.UnaryCall) error {
	return h.response(ctx, c)
}

func TestUnaryFramedBodyFirstAdmissionResponseAndDecodeFailure(t *testing.T) {
	testPayloadBodyFirst(t, transport.Framed)
}

func TestUnaryBufferedBodyFirstAdmissionResponseAndDecodeFailure(t *testing.T) {
	testPayloadBodyFirst(t, transport.PurePayload)
}

func testPayloadBodyFirst(t *testing.T, protocol transport.Protocol) {
	sp := &framedPayload{PayloadCodec: thriftcodec.NewThriftCodec()}
	cp := &framedPayload{PayloadCodec: thriftcodec.NewThriftCodec()}
	var begins, responses, finishes atomic.Int32
	sh := &framedHooks{}
	sh.begin = func(ctx context.Context, c *remote.UnaryCall) error {
		begins.Add(1)
		if sp.reads.Load() != begins.Load() || c.Request.Data() == nil || c.RequestWire.WithTTHeader || c.RequestWire.Framed != (protocol == transport.Framed) || c.RequestWire.SeqID == 0 || c.ReadEnd.IsZero() {
			return errors.New("Framed admission lacks decoded payload/wire")
		}
		return nil
	}
	addr, handler := startUnaryServer(t, nil, server.WithUnaryLifecycle(sh), server.WithPayloadCodec(sp))
	ch := &framedHooks{}
	ch.prepare = func(ctx context.Context, c *remote.UnaryCall) error {
		remoteinfo.AsRemoteInfo(c.Request.RPCInfo().To()).SetInstance(discovery.NewInstance("tcp", addr, 1, nil))
		return nil
	}
	ch.response = func(ctx context.Context, c *remote.UnaryCall) error {
		responses.Add(1)
		if cp.reads.Load() != 1 || c.ResponseWire.WithTTHeader || c.ResponseWire.Framed != (protocol == transport.Framed) || c.ResponseWire.SeqID == 0 || c.ReadEnd.IsZero() {
			return errors.New("Framed response ran before payload decode")
		}
		return nil
	}
	ch.finish = func(ctx context.Context, c *remote.UnaryCall) error {
		n := finishes.Add(1)
		if n > 1 && (c.Err == nil || c.FailureStage != remote.UnaryFailureReceive) {
			return errors.New("decode failure lost Finish classification")
		}
		return nil
	}
	cli, err := testservice.NewClient("framed", client.WithUnaryLifecycle(ch), client.WithPayloadCodec(cp), client.WithTransportProtocol(transport.PurePayload), client.WithTransportProtocol(protocol))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cli.EchoPingPong(context.Background(), &kt.Request{Message: "ok"}); err != nil {
		t.Fatal(err)
	}
	cp.fail.Store(true)
	if _, err = cli.EchoPingPong(context.Background(), &kt.Request{Message: "client-decode"}); err == nil {
		t.Fatal("accepted client decode failure")
	}
	cp.fail.Store(false)
	sp.fail.Store(true)
	if _, err = cli.EchoPingPong(context.Background(), &kt.Request{Message: "server-decode"}); err == nil {
		t.Fatal("accepted server decode failure")
	}
	if begins.Load() != 2 || handler.calls.Load() != 2 || responses.Load() != 1 || finishes.Load() != 3 || sh.header.Load() != 0 {
		t.Fatalf("begin=%d handler=%d response=%d finish=%d early=%d", begins.Load(), handler.calls.Load(), responses.Load(), finishes.Load(), sh.header.Load())
	}
}
