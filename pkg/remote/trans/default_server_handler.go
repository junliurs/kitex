/*
 * Copyright 2021 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package trans

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/trans/internal/logbackoff"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/stats"
)

// NewDefaultSvrTransHandler to provide default impl of svrTransHandler, it can be reused in netpoll, shm-ipc, framework-sdk extensions
func NewDefaultSvrTransHandler(opt *remote.ServerOption, ext Extension) (remote.ServerTransHandler, error) {
	return NewUnarySvrTransHandler(opt, ext, nil)
}

// NewUnarySvrTransHandler opts into governance without extending ServerOption.
func NewUnarySvrTransHandler(opt *remote.ServerOption, ext Extension, lifecycle remote.UnaryLifecycle) (remote.ServerTransHandler, error) {
	if lifecycle != nil {
		if _, ok := opt.Codec.(interface {
			PrepareUnaryWire(context.Context, remote.Message, *remote.UnaryCall) error
		}); !ok {
			return nil, remote.ErrUnaryLifecycleProtocol
		}
	}
	svrHdlr := &svrTransHandler{
		lifecycle:   lifecycle,
		opt:         opt,
		codec:       opt.Codec,
		svcSearcher: opt.SvcSearcher,
		ext:         ext,
	}
	if svrHdlr.opt.TracerCtl == nil {
		// init TraceCtl when it is nil, or it will lead some unit tests panic
		svrHdlr.opt.TracerCtl = &rpcinfo.TraceController{}
	}
	return svrHdlr, nil
}

type svrTransHandler struct {
	lifecycle          remote.UnaryLifecycle
	opt                *remote.ServerOption
	svcSearcher        remote.ServiceSearcher
	inkHdlFunc         endpoint.Endpoint
	codec              remote.Codec
	transPipe          *remote.TransPipeline
	ext                Extension
	inGracefulShutdown uint32
	remoteClosedWarn   logbackoff.Exponential
}

type unaryConnKey struct{}

type unaryConnState struct {
	retiredRPCInfo bool // A late business frame owns it beyond OnInactive.
	retiredFacts   atomic.Uint32
	retiredCleanup func()
	frozenContext  context.Context
	remoteClosed   atomic.Bool
	id             uint64
	used           bool
}

const (
	retiredBusinessDone   uint32 = 1
	retiredConnectionDone uint32 = 2
)

func (s *unaryConnState) finishRetired(fact uint32) {
	previous := s.retiredFacts.Load()
	for !s.retiredFacts.CompareAndSwap(previous, previous|fact) {
		previous = s.retiredFacts.Load()
	}
	if previous&fact != 0 {
		return
	}
	if previous|fact == retiredBusinessDone|retiredConnectionDone {
		cleanup := s.retiredCleanup
		s.retiredCleanup = nil
		cleanup()
		s.frozenContext = nil
	}
}

func (s *unaryConnState) applyRemoteClosed(ri rpcinfo.RPCInfo) {
	if s != nil && s.remoteClosed.Load() {
		if from := rpcinfo.AsMutableEndpointInfo(ri.From()); from != nil {
			from.SetTag(rpcinfo.RemoteClosedTag, "1")
		}
	}
}

// Write implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) Write(ctx context.Context, conn net.Conn, sendMsg remote.Message) (nctx context.Context, err error) {
	var bufWriter remote.ByteBuffer
	failureStage := remote.UnaryFailureSend
	ri := sendMsg.RPCInfo()
	rpcinfo.Record(ctx, ri, stats.WriteStart, nil)
	defer func() {
		if call := remote.UnaryCallFromContext(ctx); call != nil {
			if err != nil {
				call.FailureStage = failureStage
			}
			if !call.WriteStart.IsZero() {
				call.WriteEnd = time.Now()
			}
			completed := call.CompleteServer(ctx, err)
			if err != nil || !call.BytesRejected {
				err = completed
			}
		}
		t.ext.ReleaseBuffer(bufWriter, err)
		rpcinfo.Record(ctx, ri, stats.WriteFinish, err)
	}()

	if methodInfo := ri.Invocation().MethodInfo(); (methodInfo != nil && methodInfo.OneWay()) ||
		remote.UnaryCallFromContext(ctx).OneWay() {
		return ctx, nil
	}

	bufWriter = t.ext.NewWriteByteBuffer(ctx, conn, sendMsg)
	if call, h := bytesLifecycle(ctx); h != nil && call.BytesReply != nil {
		call.Response = sendMsg
		call.ResponseWire = responseWire(call.BytesReply)
		call.WriteStart = time.Now()
		if err = writeFinalBytes(bufWriter, call.BytesReply); err != nil {
			return ctx, err
		}
		call.BytesReply = nil
		return ctx, bufWriter.Flush()
	}

	if call := remote.UnaryCallFromContext(ctx); call != nil {
		call.Response = sendMsg
		preparer, ok := t.codec.(interface {
			PrepareUnaryWire(context.Context, remote.Message, *remote.UnaryCall) error
		})
		if !ok {
			return ctx, remote.ErrUnaryLifecycleProtocol
		}
		if err = preparer.PrepareUnaryWire(ctx, sendMsg, call); err != nil {
			return ctx, err
		}
		if call.Started {
			if err = call.Lifecycle.ServerResponse(ctx, call); err != nil {
				failureStage = remote.UnaryFailurePrepare
				return ctx, err
			}
		}
	}

	if call := remote.UnaryCallFromContext(ctx); call != nil && call.ResponseDelay > 0 {
		if err = call.WaitResponseDelay(ctx, time.Time{}); err != nil {
			failureStage = remote.UnaryFailurePrepare
			return ctx, err
		}
	}
	if call, h := bytesLifecycle(ctx); h != nil {
		frame, owner, e := encodeTransportBytes(ctx, t.codec, sendMsg, bufWriter)
		if e != nil {
			return ctx, e
		}
		defer owner.Release(nil)
		r, e := processBytes(ctx, call, h, remote.UnaryBytesIngressResponse, frame)
		if e != nil {
			return ctx, e
		}
		defer releaseBytes(r)
		call.ResponseWire = responseWire(r.Bytes)
		if r.Action == remote.UnaryBytesReject {
			call.BytesRejected = true
			call.FailureStage = remote.UnaryFailurePrepare
		}
		call.WriteStart = time.Now()
		if e = writeTransportBytes(bufWriter, owner, frame, r.Bytes); e != nil {
			return ctx, e
		}
		return ctx, bufWriter.Flush()
	}
	if call, h := captureLifecycle(ctx); h != nil {
		if handled, e := writeCapturedMessage(ctx, t.codec, sendMsg, bufWriter, call, h, remote.UnaryBytesIngressResponse); handled {
			return ctx, e
		}
		frame, owner, e := encodeTransportBytes(ctx, t.codec, sendMsg, bufWriter)
		if e != nil {
			return ctx, e
		}
		defer owner.Release(nil)
		captureWire(ctx, call, h, remote.UnaryBytesIngressResponse, frame)
		call.WriteStart = time.Now()
		if e = writeTransportBytes(bufWriter, owner, frame, frame); e != nil {
			return ctx, e
		}
		return ctx, bufWriter.Flush()
	}

	err = t.codec.Encode(ctx, sendMsg, bufWriter)
	if err != nil {
		return ctx, err
	}
	if call := remote.UnaryCallFromContext(ctx); call != nil {
		call.WriteStart = time.Now()
	}
	return ctx, bufWriter.Flush()
}

// Read implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) Read(ctx context.Context, conn net.Conn, recvMsg remote.Message) (nctx context.Context, err error) {
	var bufReader remote.ByteBuffer
	ri := recvMsg.RPCInfo()
	defer func() {
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			panicErr := kerrors.ErrPanic.WithCauseAndStack(fmt.Errorf("[happened in Read] %s", r), stack)
			rpcinfo.AsMutableRPCStats(ri.Stats()).SetPanicked(panicErr)
			err = remote.NewTransError(remote.ProtocolError, panicErr)
			nctx = ctx
		}
		t.ext.ReleaseBuffer(bufReader, err)
		rpcinfo.Record(ctx, ri, stats.ReadFinish, err)
	}()
	rpcinfo.Record(ctx, ri, stats.ReadStart, nil)

	bufReader = t.ext.NewReadByteBuffer(ctx, conn, recvMsg)
	var capture *metaCapture
	var receivedPayload []byte
	if _, h := bytesLifecycle(ctx); h != nil {
		capture = newMetaCapture(bufReader)
		bufReader = capture
	}

	beginObservedMessage(bufReader)
	if codec, ok := t.codec.(remote.MetaDecoder); ok {
		if err = codec.DecodeMeta(ctx, recvMsg, bufReader); err == nil {
			if call := remote.UnaryCallFromContext(ctx); call != nil {
				ctx = remote.ContextForUnaryRequest(ctx, call)
				if admission, ok := call.Lifecycle.(interface {
					HeaderBeforePayload() bool
					ServerHeaderReady(context.Context, *remote.UnaryCall) error
				}); ok && call.RequestWire.WithTTHeader && admission.HeaderBeforePayload() {
					call.ReadEnd, call.Started = time.Now(), true
					call.FailureStage = remote.UnaryFailurePrepare
					if ms := rpcinfo.AsMutableRPCStats(ri.Stats()); ms != nil {
						ms.SetRecvSize(uint64(remote.ConsumedLen(bufReader) + recvMsg.PayloadLen()))
					}
					call.Err = admission.ServerHeaderReady(ctx, call)
					if call.Err != nil {
						// Consume bytes for connection reuse, without unmarshalling
						// the business payload. Read only the Thrift envelope for
						// the method/sequence needed by the error response.
						if err = prepareDeniedEnvelope(recvMsg, bufReader); err != nil {
							return ctx, err
						}
						if err = bufReader.Skip(recvMsg.PayloadLen()); err != nil {
							return ctx, err
						}
						remote.RefreshUnaryRPCInfo(recvMsg, conn)
						return ctx, nil
					}
					call.FailureStage = remote.UnaryFailureNone
				}
			}
			if call := remote.UnaryCallFromContext(ctx); call != nil && !call.RequestWire.WithTTHeader && call.RequestWire.Framed && !supportsOriginalCapture(call, bufReader) {
				// Keep a borrowed view while the read buffer is retained. Admission
				// needs decoded Base; selected recording still uses the original wire.
				var e error
				receivedPayload, e = bufReader.Peek(recvMsg.PayloadLen())
				call.ReadEnd = time.Now()
				if e != nil {
					return ctx, e
				}
			}
			if call, h := captureLifecycle(ctx); h != nil && call.RequestWire.WithTTHeader {
				var payload []byte
				var e error
				if !supportsOriginalCapture(call, bufReader) {
					payload, e = bufReader.Peek(recvMsg.PayloadLen())
				}
				if e != nil {
					return ctx, e // Actual payload read failed; do not read it a second time.
				}
				captureReceivedPayload(ctx, recvMsg, call, h, remote.UnaryBytesIngressRequest, payload, call.RequestWire, bufReader)
			}
			if t.opt.Profiler != nil && t.opt.ProfilerTransInfoTagging != nil && recvMsg.TransInfo() != nil {
				var tags []string
				ctx, tags = t.opt.ProfilerTransInfoTagging(ctx, recvMsg)
				ctx = t.opt.Profiler.Tag(ctx, tags...)
			}

			if call, h := bytesLifecycle(ctx); h != nil {
				frame, e := capture.frame(recvMsg.PayloadLen())
				if e != nil {
					return ctx, e
				}
				r, e := processBytes(ctx, call, h, remote.UnaryBytesIngressRequest, frame)
				if e != nil {
					return ctx, e
				}
				if r.ReuseDecodedMetadata && r.Action == remote.UnaryBytesContinue {
					releaseBytes(r)
					capture.payload = true
				} else {
					final := capture.retainResult(r, frame)
					action := r.Action
					releaseBytes(r)
					if e = capture.discardPayload(recvMsg.PayloadLen()); e != nil {
						return ctx, e
					}
					_ = t.ext.ReleaseBuffer(bufReader, nil)
					bufReader = nil
					if action != remote.UnaryBytesContinue {
						call.BytesReply = final
						call.BytesRejected = action == remote.UnaryBytesReject
						call.ResponseWire = responseWire(final)
						return ctx, nil
					}
					bufReader = remote.NewReaderBuffer(final)
					remote.ResetUnaryDecodedHeaders(recvMsg)
					if e = codec.DecodeMeta(ctx, recvMsg, bufReader); e != nil {
						return ctx, e
					}
				}
			}
			err = codec.DecodePayload(ctx, recvMsg, bufReader)
		}
	} else {
		err = t.codec.Decode(ctx, recvMsg, bufReader)
	}
	if err != nil {
		recvMsg.Tags()[remote.ReadFailed] = true
		return ctx, err
	}
	if call := remote.UnaryCallFromContext(ctx); call != nil {
		if err = ctx.Err(); err != nil {
			// The decoder may finish after peer cancellation. Keep its buffers
			// alive through DecodePayload, then stop before invoking business.
			call.FailureStage = remote.UnaryFailureReceive
			return ctx, err
		}
		if err = remote.CheckUnaryLifecycle(ri); err != nil {
			return ctx, err
		}
		if call.ReadEnd.IsZero() {
			call.ReadEnd = time.Now()
		}
		call.Started = true
		call.FailureStage = remote.UnaryFailurePrepare
		call.Err = call.Lifecycle.ServerBegin(ctx, call)
		if call.Err == nil {
			if value, ok := recvMsg.Data().(interface{ MaterializePayload() error }); ok {
				call.Err = value.MaterializePayload()
			}
		}
		if call.Err == nil && !call.RequestWire.WithTTHeader {
			captureDecodedMessage(ctx, bufReader, call, remote.UnaryBytesIngressRequest)
		}
		if call.Err == nil && receivedPayload != nil {
			if _, h := captureLifecycle(ctx); h != nil {
				captureReceivedPayload(ctx, recvMsg, call, h, remote.UnaryBytesIngressRequest, receivedPayload, call.RequestWire)
			}
		}
		if call.Err == nil {
			call.FailureStage = remote.UnaryFailureNone
		}
		remote.RefreshUnaryRPCInfo(recvMsg, conn)
	}
	return ctx, nil
}

func (t *svrTransHandler) newCtxWithRPCInfo(ctx context.Context, conn net.Conn) (context.Context, rpcinfo.RPCInfo) {
	var ri rpcinfo.RPCInfo
	if rpcinfo.PoolEnabled() { // reuse per-connection rpcinfo
		ri = rpcinfo.GetRPCInfo(ctx)
		// delayed reinitialize for faster response
	} else {
		// new rpcinfo if reuse is disabled
		ri = t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
		ctx = rpcinfo.NewCtxWithRPCInfo(ctx, ri)
	}
	if atomic.LoadUint32(&t.inGracefulShutdown) == 1 {
		// If server is in graceful shutdown status, mark connection reset flag to all responses to let client close the connections.
		if ei := rpcinfo.AsTaggable(ri.To()); ei != nil {
			ei.SetTag(rpcinfo.ConnResetTag, "1")
		}
	}
	return ctx, ri
}

// OnRead implements the remote.ServerTransHandler interface.
// The connection should be closed after returning error.
func (t *svrTransHandler) OnRead(ctx context.Context, conn net.Conn) (err error) {
	ctx, ri := t.newCtxWithRPCInfo(ctx, conn)
	t.ext.SetReadTimeout(ctx, conn, ri.Config(), remote.Server)
	var recvMsg remote.Message
	var sendMsg remote.Message
	var call *remote.UnaryCall
	if t.lifecycle != nil {
		call = remote.NewUnaryCall(t.lifecycle)
		call.Conn = conn
		if state, ok := ctx.Value(unaryConnKey{}).(*unaryConnState); ok {
			call.ConnID, call.Reused = state.id, state.used
			state.used = true
		}
		ctx = remote.WithUnaryCall(ctx, call)
	}
	closeConnOutsideIfErr := true
	defer func() {
		var panicErr error
		if r := recover(); r != nil {
			stack := string(debug.Stack())
			if conn != nil {
				ri := rpcinfo.GetRPCInfo(ctx)
				rService, rAddr := getRemoteInfo(ri, conn)
				klog.CtxErrorf(ctx, "KITEX: panic happened, remoteAddress=%s, remoteService=%s, error=%v\nstack=%s", rAddr, rService, r, stack)
			} else {
				klog.CtxErrorf(ctx, "KITEX: panic happened, error=%v\nstack=%s", r, stack)
			}
			panicErr = kerrors.ErrPanic.WithCauseAndStack(fmt.Errorf("[happened in OnRead] %v", r), stack)
			if err == nil {
				err = panicErr
			}
		}
		if call != nil && call.SealBusiness() {
			state, _ := ctx.Value(unaryConnKey{}).(*unaryConnState)
			if state != nil {
				state.retiredRPCInfo = true
				if state.frozenContext == nil {
					state.frozenContext = rpcinfo.FreezeRPCInfo(ctx)
				}
			}
			if err == nil {
				err = errors.New("unary business is still running after reply")
			}
			call.DeferComplete = true
			err = call.CompleteServer(ctx, err)
			finalErr := err
			t.finishProfiler(ctx)
			cleanup := func() {
				state.applyRemoteClosed(ri)
				t.finishTracer(ctx, ri, finalErr, conn, panicErr)
				extra := call.Response
				if extra != nil && extra != recvMsg && extra != sendMsg {
					remote.RecycleMessage(extra)
				}
				remote.RecycleMessage(recvMsg)
				remote.RecycleMessage(sendMsg)
				call.Recycle()
				rpcinfo.PutRPCInfo(ri)
			}
			if state != nil {
				state.retiredCleanup = cleanup
			}
			var notify func()
			if h, ok := call.Lifecycle.(remote.UnaryServerBusinessLifecycle); ok {
				notify = func() {
					call.SnapshotBusiness()
					if e := h.ServerBusinessCompleted(ctx, call); e != nil {
						finalErr = errors.Join(finalErr, e)
					}
				}
			}
			call.RetireAfterBusinessWithNotification(notify, func() {
				call.DeferComplete = false
				call.SnapshotBusiness()
				finalErr = call.CompleteServer(ctx, finalErr)
				if state != nil {
					state.finishRetired(retiredBusinessDone)
				} else {
					cleanup()
				}
			})
			return
		}
		if call != nil {
			call.DeferComplete = false
			call.SnapshotBusiness()
		}
		if call != nil {
			if err != nil && call.FailureStage == remote.UnaryFailureNone {
				call.FailureStage = remote.UnaryFailureSend
			}
			if call.FailureStage == remote.UnaryFailureSend && call.Err != nil {
				err = call.Err
			}
			err = call.CompleteServer(ctx, err)
		}
		if state, ok := ctx.Value(unaryConnKey{}).(*unaryConnState); ok {
			state.applyRemoteClosed(ri)
		}
		t.finishTracer(ctx, ri, err, conn, panicErr)
		t.finishProfiler(ctx)
		closeAfterReply := call != nil && call.CloseAfterReply
		if call != nil && call.Response != nil && call.Response != recvMsg && call.Response != sendMsg {
			remote.RecycleMessage(call.Response)
		}
		if call != nil {
			call.Recycle()
		}
		remote.RecycleMessage(recvMsg)
		remote.RecycleMessage(sendMsg)
		// reset rpcinfo for reuse
		if rpcinfo.PoolEnabled() {
			t.opt.InitOrResetRPCInfoFunc(ri, conn.RemoteAddr())
		}
		if err != nil && !closeConnOutsideIfErr && !closeAfterReply {
			// when error is not nil, outside will close conn,
			// set err to nil to indicate that this kind of error does not require closing the connection
			err = nil
		}
	}()
	ctx = t.startTracer(ctx, ri)
	ctx = t.startProfiler(ctx)
	recvMsg = remote.NewMessage(nil, ri, remote.Call, remote.Server)
	if call != nil {
		call.Request = recvMsg
	}
	recvMsg.SetPayloadCodec(t.opt.PayloadCodec)
	ctx, err = t.transPipe.Read(ctx, conn, recvMsg)
	if err != nil {
		t.writeErrorReplyIfNeeded(ctx, recvMsg, conn, err, ri, true, true)
		// t.OnError(ctx, err, conn) will be executed at outer function when transServer close the conn
		return err
	}
	if call != nil && call.Err != nil {
		err = remote.NewTransError(remote.InternalError, call.Err)
		closeConnOutsideIfErr = t.writeErrorReplyIfNeeded(ctx, recvMsg, conn, err, ri, true, false)
		return err
	}

	if call != nil && call.BytesReply != nil {
		sendMsg = remote.NewMessage(nil, ri, remote.Reply, remote.Server)
		sendMsg.SetPayloadCodec(t.opt.PayloadCodec)
		if call.BytesRejected {
			call.FailureStage = remote.UnaryFailurePrepare
			call.Err = errors.New("meshlib: rejected byte request")
		}
		ctx, err = t.transPipe.Write(ctx, conn, sendMsg)
		return err
	}
	// heartbeat processing
	// recvMsg.MessageType would be set to remote.Heartbeat in previous Read procedure
	// if specified codec support heartbeat
	if recvMsg.MessageType() == remote.Heartbeat {
		sendMsg = remote.NewMessage(nil, ri, remote.Heartbeat, remote.Server)
	} else {
		// reply processing
		methodInfo := ri.Invocation().MethodInfo()
		if methodInfo.OneWay() {
			sendMsg = remote.NewMessage(nil, ri, remote.Reply, remote.Server)
		} else {
			sendMsg = remote.NewMessage(methodInfo.NewResult(), ri, remote.Reply, remote.Server)
		}

		ctx, err = t.transPipe.OnMessage(ctx, recvMsg, sendMsg)
		if call != nil && ctx.Err() != nil {
			// An uncooperative handler may return after disconnect. Its frame
			// stays owned until here; CompleteServer releases it without a reply.
			call.Err, call.FailureStage = ctx.Err(), remote.UnaryFailureBusiness
			return ctx.Err()
		}
		if err != nil {
			if call != nil {
				call.Err, call.FailureStage = err, remote.UnaryFailureBusiness
			}
			// error cannot be wrapped to print here, so it must exec before NewTransError
			t.OnError(ctx, err, conn)
			err = remote.NewTransError(remote.InternalError, err)
			if closeConn := t.writeErrorReplyIfNeeded(ctx, recvMsg, conn, err, ri, false, false); closeConn {
				return err
			}
			// connection don't need to be closed when the error is return by the server handler
			closeConnOutsideIfErr = false
			return err
		}
	}

	if call != nil && call.OneWay() {
		return nil
	}
	sendMsg.SetPayloadCodec(t.opt.PayloadCodec)
	if ctx, err = t.transPipe.Write(ctx, conn, sendMsg); err != nil {
		// t.OnError(ctx, err, conn) will be executed at outer function when transServer close the conn
		return err
	}
	return err
}

// OnMessage implements the remote.ServerTransHandler interface.
// msg is the decoded instance, such as Arg and Result.
func (t *svrTransHandler) OnMessage(ctx context.Context, args, result remote.Message) (context.Context, error) {
	err := t.inkHdlFunc(ctx, args.Data(), result.Data())
	if call := remote.UnaryCallFromContext(ctx); call != nil {
		call.SnapshotBusiness()
		if call.BusinessPending() {
			call.DeferComplete, call.CloseAfterReply = true, true
			if state, ok := ctx.Value(unaryConnKey{}).(*unaryConnState); ok && state.frozenContext == nil {
				state.frozenContext = rpcinfo.FreezeRPCInfo(ctx)
			}
			if err == nil {
				err = errors.New("unary middleware returned while business was still running")
			}
		}
	}
	return ctx, err
}

// OnActive implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) OnActive(ctx context.Context, conn net.Conn) (context.Context, error) {
	if t.lifecycle != nil {
		ctx = remote.WithUnaryConnectionCancellation(ctx)
		ctx = context.WithValue(ctx, unaryConnKey{}, &unaryConnState{id: remote.NextUnaryConnID()})
	}
	ctx = remote.WithServiceSearcher(ctx, t.svcSearcher)
	// init rpcinfo
	ri := t.opt.InitOrResetRPCInfoFunc(nil, conn.RemoteAddr())
	return rpcinfo.NewCtxWithRPCInfo(ctx, ri), nil
}

// OnInactive implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) OnInactive(ctx context.Context, conn net.Conn) {
	remote.CancelUnaryConnection(ctx)
	// A retired connection can leave a handler executing on its original RI.
	if state, ok := ctx.Value(unaryConnKey{}).(*unaryConnState); ok {
		if state.retiredRPCInfo {
			state.finishRetired(retiredConnectionDone)
			return
		}
		state.frozenContext = nil
	}
	// recycle rpcinfo
	rpcinfo.PutRPCInfo(rpcinfo.GetRPCInfo(ctx))
}

// OnError implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) OnError(ctx context.Context, err error, conn net.Conn) {
	state, _ := ctx.Value(unaryConnKey{}).(*unaryConnState)
	if state != nil && state.frozenContext != nil {
		ctx = state.frozenContext
	}
	ri := rpcinfo.GetRPCInfo(ctx)
	rService, rAddr := getRemoteInfo(ri, conn)
	if remoteClosedErr := IsRemoteClosedErr(t.ext, err, conn); remoteClosedErr != nil {
		// it should not regard error which cause by remote connection closed as server error
		if count, elapsed, allow := t.remoteClosedWarn.Observe(time.Now()); allow {
			klog.CtxWarnf(ctx, "KITEX: processing request error caused by remote connection closed, source=%s, remoteService=%s, remoteAddr=%v, error=%s, count=%d, period=%s", remoteClosedErr.Source, rService, rAddr, remoteClosedErr.Cause, count, elapsed)
		}
		if ri == nil {
			return
		}
		if from := rpcinfo.AsMutableEndpointInfo(ri.From()); from != nil {
			from.SetTag(rpcinfo.RemoteClosedTag, "1")
		} else if state != nil {
			state.remoteClosed.Store(true)
		}
	} else {
		var de *kerrors.DetailedError
		if ok := errors.As(err, &de); ok && de.Stack() != "" {
			klog.CtxErrorf(ctx, "KITEX: processing request error, remoteService=%s, remoteAddr=%v, error=%s\nstack=%s", rService, rAddr, err.Error(), de.Stack())
		} else {
			klog.CtxErrorf(ctx, "KITEX: processing request error, remoteService=%s, remoteAddr=%v, error=%s", rService, rAddr, err.Error())
		}
	}
}

// SetInvokeHandleFunc implements the remote.InvokeHandleFuncSetter interface.
func (t *svrTransHandler) SetInvokeHandleFunc(inkHdlFunc endpoint.Endpoint) {
	t.inkHdlFunc = inkHdlFunc
}

// SetPipeline implements the remote.ServerTransHandler interface.
func (t *svrTransHandler) SetPipeline(p *remote.TransPipeline) {
	t.transPipe = p
}

func (t *svrTransHandler) writeErrorReplyIfNeeded(
	ctx context.Context, recvMsg remote.Message, conn net.Conn, err error, ri rpcinfo.RPCInfo, doOnMessage, connReset bool,
) (shouldCloseConn bool) {
	if cn, ok := conn.(remote.IsActive); ok && !cn.IsActive() {
		// conn is closed, no need reply
		return shouldCloseConn
	}
	if methodInfo := ri.Invocation().MethodInfo(); (methodInfo != nil && methodInfo.OneWay()) ||
		remote.UnaryCallFromContext(ctx).OneWay() {
		return shouldCloseConn
	}

	transErr, isTransErr := err.(*remote.TransError)
	if !isTransErr {
		return shouldCloseConn
	}
	errMsg := remote.NewMessage(transErr, ri, remote.Exception, remote.Server)
	errMsg.SetPayloadCodec(t.opt.PayloadCodec)
	if doOnMessage {
		// if error happen before normal OnMessage, exec it to transfer header trans info into rpcinfo
		t.transPipe.OnMessage(ctx, recvMsg, errMsg)
	}
	lateReply := remote.UnaryCallFromContext(ctx) != nil && remote.UnaryCallFromContext(ctx).CloseAfterReply
	if lateReply {
		remote.AccessHeaders(errMsg.TransInfo()).SetString(transmeta.HeaderConnectionReadyToReset, "1")
	}
	if connReset && !lateReply {
		// if connection needs to be closed, set ConnResetTag to response header
		// to ensure the client won't reuse the connection.
		if ei := rpcinfo.AsTaggable(ri.To()); ei != nil {
			ei.SetTag(rpcinfo.ConnResetTag, "1")
		}
	}
	ctx, err = t.transPipe.Write(ctx, conn, errMsg)
	if err != nil {
		if call := remote.UnaryCallFromContext(ctx); call != nil {
			call.Err, call.FailureStage = err, remote.UnaryFailureSend
		}
		klog.CtxErrorf(ctx, "KITEX: write error reply failed, remote=%s, error=%s", conn.RemoteAddr(), err.Error())
		return true
	}
	return shouldCloseConn
}

func (t *svrTransHandler) startTracer(ctx context.Context, ri rpcinfo.RPCInfo) context.Context {
	c := t.opt.TracerCtl.DoStart(ctx, ri)
	return c
}

func (t *svrTransHandler) finishTracer(ctx context.Context, ri rpcinfo.RPCInfo, err error, conn net.Conn, panicErr interface{}) {
	rpcStats := rpcinfo.AsMutableRPCStats(ri.Stats())
	if rpcStats == nil {
		return
	}
	if panicErr != nil {
		rpcStats.SetPanicked(panicErr)
	}
	if remoteClosedErr := IsRemoteClosedErr(t.ext, err, conn); remoteClosedErr != nil &&
		remoteClosedErr.Source == RemoteClosedByExtension {
		// it should not regard the error which caused by remote connection closed as server error
		err = nil
	}
	t.opt.TracerCtl.DoFinish(ctx, ri, err)
	// for server side, rpcinfo is reused on connection, clear the rpc stats info but keep the level config
	sl := ri.Stats().Level()
	rpcStats.Reset()
	rpcStats.SetLevel(sl)
}

func (t *svrTransHandler) startProfiler(ctx context.Context) context.Context {
	if t.opt.Profiler == nil {
		return ctx
	}
	return t.opt.Profiler.Prepare(ctx)
}

func (t *svrTransHandler) finishProfiler(ctx context.Context) {
	if t.opt.Profiler == nil {
		return
	}
	t.opt.Profiler.Untag(ctx)
}

func (t *svrTransHandler) GracefulShutdown(ctx context.Context) error {
	atomic.StoreUint32(&t.inGracefulShutdown, 1)
	return nil
}

func getRemoteInfo(ri rpcinfo.RPCInfo, conn net.Conn) (string, net.Addr) {
	rAddr := conn.RemoteAddr()
	if ri == nil {
		return "", rAddr
	}
	if rAddr != nil && rAddr.Network() == "unix" {
		if ri.From().Address() != nil {
			rAddr = ri.From().Address()
		}
	}
	return ri.From().ServiceName(), rAddr
}

// UnaryLifecycle is queried once when configuring the network server.
func (t *svrTransHandler) UnaryLifecycle() remote.UnaryLifecycle { return t.lifecycle }
