package remote

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
)

var ErrUnaryTransportProtocol = errors.New("custom unary transport does not support streaming")

// UnaryTaskRunner runs a transport task with Kitex's timeout worker. The task
// owns its arguments until it exits, including when the caller times out first.
type UnaryTaskRunner func(context.Context, time.Duration, any, any, endpoint.Endpoint) (context.Context, error)

// UnaryTransport replaces the unary endpoint and owns resolution and deadlines.
// Middleware, retry and fallback continue to surround the endpoint.
type UnaryTransport interface {
	Validate(*serviceinfo.ServiceInfo, rpcinfo.RPCConfig) error
	NewEndpoint(*ClientOption, []MetaHandler, ClientTransHandler, UnaryTaskRunner) (endpoint.Endpoint, error)
}

// ClientPoolLifecycle binds an extension to the actual configured pool. Its
// cleanup runs before the pool closes, including failed client initialization.
type ClientPoolLifecycle interface {
	ClientPool(ConnPool) (func(), error)
}

// DisconnectHandler receives peer disconnection while a request may still run.
// OnInactive remains the final connection cleanup callback.
type DisconnectHandler interface {
	OnDisconnect(context.Context, net.Conn)
}

// WireInfo describes the envelope independently of any transport policy.
type WireInfo struct {
	WithTTHeader bool
	Framed       bool
	SeqID        int32
	ProtocolID   uint16
	Flags        uint16
	MessageType  uint32
}

type MessageEvent uint8

const (
	MessageEncode MessageEvent = iota
	MessageDecodeHeader
	MessageDecodeMeta
	MessageDecodePayload
	MessageDecodePayloadDone
)

// MessageEventHandler observes codec boundaries. Encode supplies the outgoing
// envelope; DecodeHeader supplies the raw TTHeader fields; DecodeMeta supplies
// detected framing and the Thrift message type. Payload events carry Message.
// A completion error replaces success but preserves an existing decoder error.
type MessageEventHandler interface {
	OnMessageEvent(context.Context, Message, MessageEvent, WireInfo) error
}

// TransportContextKey permits a transport's existing context wrapper to expose
// hooks without allocating another context for every codec event.
type TransportContextKey struct{}

func MessageEventHandlerFromContext(ctx context.Context) MessageEventHandler {
	h, _ := ctx.Value(TransportContextKey{}).(MessageEventHandler)
	return h
}

type InvocationCompletion interface{ End(error) }

// InvocationEventHandler brackets the actual business handler, including when
// middleware executes it asynchronously and returns before that handler exits.
type InvocationEventHandler interface {
	BeginInvocation(context.Context) (InvocationCompletion, error)
}

func BeginInvocation(ctx context.Context) (InvocationCompletion, error) {
	if h, ok := ctx.Value(TransportContextKey{}).(InvocationEventHandler); ok {
		return h.BeginInvocation(ctx)
	}
	return nil, nil
}

// MetaPrepared marks metadata written by an endpoint before the bound pipeline.
const MetaPrepared = "kitex.meta_prepared"

var connID uint64

func NextConnID() uint64 { return atomic.AddUint64(&connID, 1) }
