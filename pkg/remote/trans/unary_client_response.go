package trans

import (
	"context"
	"net"
	"time"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// readGovernedResponse hands off once metadata is available: before TTHeader
// payload decoding, or after Framed payload decoding for BaseResp/NONE. Even
// on failure it returns the live buffer to Read's existing cleanup/Finish path.
func (t *cliTransHandler) readGovernedResponse(ctx context.Context, conn net.Conn, msg remote.Message, call *remote.UnaryCall, response remote.UnaryClientResponseLifecycle, cleanup *remote.ByteBuffer) (reader remote.ByteBuffer, err error) {
	defer func() { *cleanup = reader }() // Preserve ownership through decoder panic.
	codec, ok := t.codec.(remote.MetaDecoder)
	if !ok {
		return nil, remote.ErrUnaryLifecycleProtocol
	}
	msg.SetPayloadCodec(t.opt.PayloadCodec)
	localReply := call.BytesReply != nil
	if localReply {
		reader = remote.NewReaderBuffer(call.BytesReply)
		call.BytesReply = nil
	} else {
		reader = t.ext.NewReadByteBuffer(ctx, conn, msg)
	}
	if !localReply && call.ResponseDelay > 0 {
		if _, err = reader.Peek(1); err != nil {
			return reader, err
		}
		if err = call.WaitResponseDelay(ctx, time.Now()); err != nil {
			return reader, err
		}
	}
	var capture *metaCapture
	_, bytesHandler := bytesLifecycle(ctx)
	if bytesHandler != nil && !localReply {
		capture = newMetaCapture(reader)
		reader = capture
	}
	beginObservedMessage(reader)
	if err = codec.DecodeMeta(ctx, msg, reader); err != nil {
		return reader, err
	}
	wireSize := uint64(remote.ConsumedLen(reader) + msg.PayloadLen())
	if !call.ResponseWire.WithTTHeader {
		if localReply {
			if stats := rpcinfo.AsMutableRPCStats(msg.RPCInfo().Stats()); stats != nil {
				defer stats.SetRecvSize(0)
			}
		}
		var legacyPayload []byte
		if _, h := captureLifecycle(ctx); h != nil && !supportsOriginalCapture(call, reader) && call.ResponseWire.Framed {
			legacyPayload, err = reader.Peek(msg.PayloadLen())
			if err != nil {
				return reader, err
			}
		}
		// Buffered has no length prefix. The existing codec owns boundary
		// discovery and business decoding; do not add a second skip/decode.
		if err = codec.DecodePayload(ctx, msg, reader); err != nil {
			return reader, err
		}
		call.ReadEnd = time.Now()
		if err = ctx.Err(); err != nil {
			return reader, err
		}
		if err = response.ClientResponse(ctx, call); err != nil {
			return reader, err
		}
		if !localReply {
			if _, h := captureLifecycle(ctx); h != nil {
				if !captureOriginalMessage(ctx, reader, call, h, remote.UnaryBytesEgressResponse, 0) && legacyPayload != nil {
					captureReceivedPayload(ctx, msg, call, h, remote.UnaryBytesEgressResponse, legacyPayload, call.ResponseWire)
				}
			}
		}
		return reader, nil
	}
	// Wait for the existing payload without creating a contiguous copy.
	var payload []byte
	if wait, ok := reader.(interface{ WaitReadable(int) error }); ok {
		err = wait.WaitReadable(msg.PayloadLen())
	} else {
		payload, err = reader.Peek(msg.PayloadLen())
	}
	if err != nil {
		return reader, err
	}
	if msg.PayloadLen() < 4 {
		return reader, remote.ErrUnaryLifecycleProtocol
	}
	call.ReadEnd = time.Now()
	if stats := rpcinfo.AsMutableRPCStats(msg.RPCInfo().Stats()); stats != nil {
		received := wireSize
		if localReply {
			received = 0
		}
		stats.SetRecvSize(received)
		defer func() { stats.SetRecvSize(received) }()
	}
	if err = ctx.Err(); err != nil {
		return reader, err
	}
	if err = response.ClientResponse(ctx, call); err != nil {
		return reader, err
	}

	if err = ctx.Err(); err != nil {
		return reader, err
	}
	if capture != nil {
		frame, e := capture.frame(msg.PayloadLen())
		if e != nil {
			return reader, e
		}
		r, e := processBytes(ctx, call, bytesHandler, remote.UnaryBytesEgressResponse, frame)
		if e != nil {
			return reader, e
		}
		call.BytesRejected = r.Action == remote.UnaryBytesReject
		if r.ReuseDecodedMetadata && r.Action == remote.UnaryBytesContinue {
			releaseBytes(r)
			capture.payload = true
		} else {
			final := capture.retainResult(r, frame)
			releaseBytes(r)
			if e = capture.discardPayload(msg.PayloadLen()); e != nil {
				return reader, e
			}
			_ = t.ext.ReleaseBuffer(reader, nil)
			reader = remote.NewReaderBuffer(final)
			remote.ResetUnaryDecodedHeaders(msg)
			if e = codec.DecodeMeta(ctx, msg, reader); e != nil {
				return reader, e
			}
		}
	}
	if c, h := captureLifecycle(ctx); h != nil && !localReply {
		captureReceivedPayload(ctx, msg, c, h, remote.UnaryBytesEgressResponse, payload, call.ResponseWire, reader)
	}
	return reader, codec.DecodePayload(ctx, msg, reader)
}
