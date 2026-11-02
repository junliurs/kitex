package trans

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/cloudwego/gopkg/protocol/ttheader"

	"github.com/cloudwego/kitex/pkg/remote"
)

func captureLifecycle(ctx context.Context) (*remote.UnaryCall, remote.UnaryCaptureLifecycle) {
	c := remote.UnaryCallFromContext(ctx)
	if c == nil {
		return nil, nil
	}
	h, _ := c.Lifecycle.(remote.UnaryCaptureLifecycle)
	if h == nil || !h.CaptureRequested(c) {
		return c, nil
	}
	return c, h
}

func captureWire(ctx context.Context, c *remote.UnaryCall, h remote.UnaryCaptureLifecycle, stage remote.UnaryBytesStage, frame []byte) {
	// Observation cannot change network/codec behavior, including observer panic.
	defer func() { _ = recover() }()
	h.CaptureFrame(ctx, c, stage, frame)
}

func captureReceivedPayload(ctx context.Context, msg remote.Message, c *remote.UnaryCall, h remote.UnaryCaptureLifecycle, stage remote.UnaryBytesStage, payload []byte, w remote.UnaryWire, readers ...remote.ByteBuffer) {
	defer func() { _ = recover() }()
	if len(readers) > 0 {
		if captureOriginalMessage(ctx, readers[0], c, h, stage, msg.PayloadLen()) {
			return
		}
		if payload == nil {
			var err error
			payload, err = readers[0].Peek(msg.PayloadLen())
			if err != nil {
				return
			}
		}
	}
	b := newFrameWriter()
	defer b.Release(nil)
	if !w.WithTTHeader {
		if !w.Framed {
			captureWire(ctx, c, h, stage, payload)
			return
		}
		size, err := b.Malloc(4)
		if err != nil {
			return
		}
		binary.BigEndian.PutUint32(size, uint32(len(payload)))
		if _, err = b.WriteBinary(payload); err != nil {
			return
		}
		frame, err := b.Bytes()
		if err == nil {
			captureWire(ctx, c, h, stage, frame)
		}
		return
	}
	params := ttheader.EncodeParam{Flags: ttheader.HeaderFlags(w.Flags), SeqID: w.SeqID, ProtocolID: ttheader.ProtocolID(w.ProtocolID)}
	var length []byte
	var err error
	headers := remote.AccessHeaders(msg.TransInfo())
	length, err = headers.EncodeTTHeader(ctx, params, b)
	if err != nil {
		return
	}
	if w.Framed {
		size, _ := b.Malloc(4)
		binary.BigEndian.PutUint32(size, uint32(len(payload)))
	}
	_, _ = b.WriteBinary(payload)
	binary.BigEndian.PutUint32(length, uint32(b.WrittenLen()-4))
	frame, err := b.Bytes()
	if err == nil {
		captureWire(ctx, c, h, stage, frame)
	}
}

type borrowedMessageReader interface {
	BeginBorrowedMessage()
	BorrowConsumedMessage([][]byte) [][]byte
}

func beginObservedMessage(reader remote.ByteBuffer) {
	if b, ok := reader.(borrowedMessageReader); ok {
		b.BeginBorrowedMessage()
	}
}

func captureOriginalMessage(ctx context.Context, reader remote.ByteBuffer, c *remote.UnaryCall, h remote.UnaryCaptureLifecycle, stage remote.UnaryBytesStage, remaining int) bool {
	observer, ok := h.(remote.UnaryCaptureSegmentsLifecycle)
	b, supported := reader.(interface {
		BorrowMessage(int, [][]byte) ([][]byte, error)
	})
	if !ok || !supported {
		return false
	}
	if state, ok := reader.(interface{ CanBorrowMessage() bool }); ok && !state.CanBorrowMessage() {
		return false
	}
	// Observing bytes cannot replace a business outcome, including on panic.
	func() {
		defer func() { _ = recover() }()
		var scratch [16][]byte
		parts, err := b.BorrowMessage(remaining, scratch[:0])
		if err == nil && len(parts) > 0 {
			observer.CaptureSegments(ctx, c, stage, parts)
			borrowedReadFrames.Add(1)
		}
	}()
	return true
}

func captureDecodedMessage(ctx context.Context, reader remote.ByteBuffer, c *remote.UnaryCall, stage remote.UnaryBytesStage) {
	if _, h := captureLifecycle(ctx); h != nil {
		captureOriginalMessage(ctx, reader, c, h, stage, 0)
	}
}

func writeCapturedMessage(ctx context.Context, codec remote.Codec, msg remote.Message, out remote.ByteBuffer, c *remote.UnaryCall, h remote.UnaryCaptureLifecycle, stage remote.UnaryBytesStage) (bool, error) {
	observer, ok := h.(remote.UnaryCaptureSegmentsLifecycle)
	t, native := out.(unaryFrameTransport)
	factory, borrow := out.(interface{ NewCaptureFrameBuffer() remote.ByteBuffer })
	if !ok || !native || !borrow {
		return false, nil
	}
	owner := factory.NewCaptureFrameBuffer()
	defer owner.Release(nil)
	views, ok := owner.(interface {
		CaptureSegments([][]byte) ([][]byte, error)
	})
	if !ok {
		return false, nil
	}
	if err := codec.Encode(ctx, msg, owner); err != nil {
		return true, err
	}
	var scratch [16][]byte
	parts, err := views.CaptureSegments(scratch[:0])
	if err != nil {
		return true, err
	}
	func() { defer func() { _ = recover() }(); observer.CaptureSegments(ctx, c, stage, parts) }()
	c.WriteStart = time.Now()
	if err = t.CommitUnaryFrame(owner); err != nil {
		return true, err
	}
	sentFrames.Add(1)
	transferredFrames.Add(1)
	return true, out.Flush()
}

func supportsOriginalCapture(c *remote.UnaryCall, reader remote.ByteBuffer) bool {
	_, observer := c.Lifecycle.(remote.UnaryCaptureSegmentsLifecycle)
	_, borrow := reader.(interface {
		BorrowMessage(int, [][]byte) ([][]byte, error)
	})
	if state, ok := reader.(interface{ CanBorrowMessage() bool }); ok && !state.CanBorrowMessage() {
		return false
	}
	return observer && borrow
}
