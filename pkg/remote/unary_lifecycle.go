package remote

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/pkg/utils"
	"github.com/cloudwego/kitex/transport"
)

var ErrUnaryLifecycleProtocol = errors.New("unary lifecycle requires Buffered, TTHeader or Framed Thrift unary and the default codec")

type UnaryLifecycle interface {
	ClientPrepare(context.Context, *UnaryCall) error
	ClientFinish(context.Context, *UnaryCall) error
	ServerBegin(context.Context, *UnaryCall) error
	ServerResponse(context.Context, *UnaryCall) error
	ServerComplete(context.Context, *UnaryCall) error
}

// UnaryClientResponseLifecycle governs decoded response headers before payload
// consumption. It is optional for existing lifecycle implementations.
type UnaryClientResponseLifecycle interface {
	ClientResponse(context.Context, *UnaryCall) error
}

// Optional cold lifecycle: bind a client's actual pool to discovery removals.
// The returned unbind runs before ConnPool.Close, outside request execution.
type UnaryClientPoolLifecycle interface {
	ClientPool(ConnPool) (func(), error)
}

// UnaryServerReplyLifecycle reports the actual reply while business execution
// is still retained. ServerComplete remains the final execution retirement.
type UnaryServerReplyLifecycle interface {
	ServerReplyCompleted(context.Context, *UnaryCall) error
}

// UnaryServerBusinessLifecycle receives actual handler exit independently of
// middleware scope retirement. Calls are serialized with reply completion.
type UnaryServerBusinessLifecycle interface {
	ServerBusinessCompleted(context.Context, *UnaryCall) error
}

type UnaryFailureStage uint8

const (
	UnaryFailureNone UnaryFailureStage = iota
	UnaryFailurePrepare
	UnaryFailureConnect
	UnaryFailureSend
	UnaryFailureReceive
	UnaryFailureBusiness
)

type UnaryMetadataSource uint8

const (
	UnaryMetadataNone UnaryMetadataSource = iota
	UnaryMetadataTTHeader
	UnaryMetadataBase
	UnaryMetadataBaseResp
)

type UnaryCall struct {
	payloadExtraRaw                               string
	payloadExtraValues                            map[string]string
	payloadExtraValid                             bool
	ConnectionPolicy                              ConnectionPolicy
	ResponseDelay                                 time.Duration
	ResponseDelayFacts                            UnaryHostIOFacts
	responseDelayApplied                          bool
	RequestMetadataSource, ResponseMetadataSource UnaryMetadataSource
	BytesReply                                    []byte
	BytesRejected                                 bool
	Request, Response                             Message
	RequestWire, ResponseWire                     UnaryWire
	State                                         any
	Conn                                          net.Conn
	ConnID                                        uint64
	Err                                           error
	BusinessNotificationError                     error
	HandlerError, ReplyError                      error
	ReplyDone, DeferComplete, CloseAfterReply     bool
	replyReported                                 bool
	FailureStage                                  UnaryFailureStage
	Begin, ConnectStart, ConnectEnd               time.Time
	WriteStart, WriteEnd, ReadEnd                 time.Time
	HandlerStart, HandlerEnd                      time.Time
	Reused                                        bool
	MetaPrepared                                  bool
	Started                                       bool
	Finished                                      bool
	Lifecycle                                     UnaryLifecycle
	dialer                                        Dialer
	execution                                     *unaryExecution
}

type UnaryWire struct {
	WithTTHeader bool
	Framed       bool
	SeqID        int32
	ProtocolID   uint16
	Flags        uint16
	MessageType  uint32
}

var unaryCallPool = sync.Pool{New: func() any { return &UnaryCall{execution: new(unaryExecution)} }}

var unaryConnID uint64

func NextUnaryConnID() uint64 {
	return atomic.AddUint64(&unaryConnID, 1)
}

type (
	unaryCallKey                struct{}
	unaryConnectionCancelKey    struct{}
	unaryConnectionCancellation struct {
		parent context.Context
		cancel context.CancelFunc
	}
)

// Keep request values while ignoring only the peer's connection cancellation.
// Server lifetime and its later request timeout remain effective.
type unaryOnewayContext struct {
	context.Context
	lifetime context.Context
}

func (c unaryOnewayContext) Deadline() (time.Time, bool) { return c.lifetime.Deadline() }
func (c unaryOnewayContext) Done() <-chan struct{}       { return c.lifetime.Done() }
func (c unaryOnewayContext) Err() error                  { return c.lifetime.Err() }
func (c unaryOnewayContext) Value(key any) any {
	if value := c.Context.Value(key); value != nil {
		return value
	}
	// WithoutCancel hides the connection's cancellation internals. Restore the
	// server parent's cancellation identity/cause so derived contexts follow it.
	return c.lifetime.Value(key)
}

func ContextForUnaryRequest(ctx context.Context, call *UnaryCall) context.Context {
	if !call.OneWay() {
		return ctx
	}
	if state, ok := ctx.Value(unaryConnectionCancelKey{}).(unaryConnectionCancellation); ok {
		return unaryOnewayContext{Context: context.WithoutCancel(ctx), lifetime: state.parent}
	}
	return ctx
}

// A TTHeader unary connection has one active request. A peer disconnect can
// cancel its handler before the final close callback is allowed to run.
func WithUnaryConnectionCancellation(ctx context.Context) context.Context {
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	return context.WithValue(ctx, unaryConnectionCancelKey{}, unaryConnectionCancellation{parent: parent, cancel: cancel})
}

func CancelUnaryConnection(ctx context.Context) {
	if state, ok := ctx.Value(unaryConnectionCancelKey{}).(unaryConnectionCancellation); ok {
		state.cancel()
	}
}

func NewUnaryCall(h UnaryLifecycle) *UnaryCall {
	c := unaryCallPool.Get().(*UnaryCall)
	if c.execution == nil || (c.execution.state.Load()&^executionFlags) == ^uint64(executionFlags) {
		c.execution = new(unaryExecution)
	}
	c.execution.begin()
	c.Lifecycle = h
	c.Begin = time.Now()
	return c
}

func WithUnaryCall(ctx context.Context, c *UnaryCall) context.Context {
	if c.execution == nil {
		c.execution = new(unaryExecution)
		c.execution.begin()
	}
	return &unaryCallContext{Context: ctx, call: c, execution: c.execution, epoch: c.execution.state.Load() &^ executionFlags}
}

func UnaryCallFromContext(ctx context.Context) *UnaryCall {
	if ctx == nil {
		return nil
	}
	reference, _ := ctx.Value(unaryCallKey{}).(*unaryCallContext)
	if reference != nil && reference.live() {
		return reference.call
	}
	return nil
}

// OneWay is available before payload decoding from the Thrift envelope.
func (c *UnaryCall) OneWay() bool {
	if c == nil || c.Request == nil {
		return false
	}
	if c.Request.MessageType() == Oneway {
		return true
	}
	method := c.Request.RPCInfo().Invocation().MethodInfo()
	return method != nil && method.OneWay()
}

func (c *UnaryCall) Recycle() {
	execution := c.execution
	if execution != nil {
		execution.addFlags(executionRetired)
		execution.businessErr = nil
		execution.observer = nil
		execution.observerErr = nil
	}
	*c = UnaryCall{execution: execution}
	unaryCallPool.Put(c)
}

func (c *UnaryCall) FinishClient(ctx context.Context, err error) error {
	if !c.Started || c.Finished {
		return err
	}
	c.Finished, c.Err = true, err
	if e := c.Lifecycle.ClientFinish(ctx, c); err == nil {
		err = e
	}
	return err
}

func (c *UnaryCall) CompleteServer(ctx context.Context, err error) error {
	if !c.Started || c.Finished {
		return err
	}
	if !c.ReplyDone {
		c.ReplyDone = true
		c.ReplyError = err
	}
	if err != nil {
		c.Err = err
	}
	if c.DeferComplete {
		if h, ok := c.Lifecycle.(UnaryServerReplyLifecycle); ok && !c.replyReported {
			c.replyReported = true
			c.SnapshotBusiness()
			if e := h.ServerReplyCompleted(ctx, c); err == nil {
				err = e
			}
		}
		return err
	}
	c.SnapshotBusiness()
	if c.BusinessNotificationError != nil {
		err = errors.Join(err, c.BusinessNotificationError)
	}
	c.Finished = true
	if e := c.Lifecycle.ServerComplete(ctx, c); err == nil {
		err = e
	}
	return err
}

func (c *UnaryCall) Dialer(d Dialer) Dialer {
	c.dialer = d
	c.Reused = true
	return c
}

func (c *UnaryCall) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	c.Reused = false
	return c.dialer.DialTimeout(network, address, timeout)
}

func CheckUnaryLifecycle(ri rpcinfo.RPCInfo) error {
	tp := ri.Config().TransportProtocol()
	m := ri.Invocation().MethodInfo()
	if ri.Config().PayloadCodec() != serviceinfo.Thrift || (tp != transport.PurePayload && tp != transport.TTHeader && tp != transport.TTHeaderFramed && tp != transport.Framed) ||
		(m != nil && m.StreamingMode() != serviceinfo.StreamingNone) {
		return ErrUnaryLifecycleProtocol
	}
	return nil
}

func RefreshUnaryRPCInfo(msg Message, conn net.Conn) {
	ri := msg.RPCInfo()
	headers := AccessHeaders(msg.TransInfo())
	endpoint := ri.To()
	if msg.RPCRole() == Server {
		endpoint = ri.From()
	}
	addr := conn.RemoteAddr()
	changed := true
	if v := headers.String(transmeta.HeaderTransRemoteAddr); v != "" {
		if endpoint != nil {
			current := endpoint.Address()
			changed = current == nil || current.Network() != "tcp" || current.String() != v
		}
		if changed {
			addr = utils.NewNetAddr("tcp", v)
		}
	}
	if msg.RPCRole() == Server {
		if from := rpcinfo.AsMutableEndpointInfo(ri.From()); from != nil {
			if changed {
				from.SetAddress(addr)
			}
			from.SetServiceName(headers.Int(transmeta.FromService))
			from.SetMethod(headers.Int(transmeta.FromMethod))
		}
	} else if changed {
		if to := remoteinfo.AsRemoteInfo(ri.To()); to != nil {
			to.SetRemoteAddr(addr)
		}
	}
}

// CachePayloadExtra shares one parsing result across Base adaptation and
// legacy metadata context creation. The exact raw value guards late overrides.
func (c *UnaryCall) CachePayloadExtra(raw string, values map[string]string) {
	c.payloadExtraRaw, c.payloadExtraValues, c.payloadExtraValid = raw, values, true
}

func (c *UnaryCall) PayloadExtra(raw string) (map[string]string, bool) {
	if !c.payloadExtraValid || c.payloadExtraRaw != raw {
		return nil, false
	}
	return c.payloadExtraValues, true
}

// UnaryServerTransHandlerFactory is an opt-in constructor extension. The old
// factory and public option layouts remain unchanged.
type UnaryServerTransHandlerFactory interface {
	NewUnaryTransHandler(*ServerOption, UnaryLifecycle) (ServerTransHandler, error)
}

// UnaryLifecycleProvider exposes initialization metadata, not a request hook.
type UnaryLifecycleProvider interface{ UnaryLifecycle() UnaryLifecycle }

// ConnectionPolicyPool adds per-attempt policy without changing ConnOption.
type ConnectionPolicyPool interface {
	GetWithPolicy(context.Context, string, string, ConnOption, ConnectionPolicy) (net.Conn, error)
}
