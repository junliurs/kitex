package netpoll

import (
	"errors"
	"sync"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/remote"
)

// A private frame is not attached to the connection until the plugin accepts
// it. Append transfers its nodes, so Flush timeout cannot invalidate ownership.
type unaryFrameBuffer struct {
	remote.ByteBuffer
	buffer *netpoll.LinkBuffer
	data   []byte
	used   int
}

var unaryFrames = sync.Pool{New: func() any { return &unaryFrameBuffer{} }}

func (b *netpollByteBuffer) NewUnaryFrameBuffer() remote.ByteBuffer {
	f := unaryFrames.Get().(*unaryFrameBuffer)
	f.buffer = netpoll.NewLinkBuffer()
	return f
}

func (b *netpollByteBuffer) CommitUnaryFrame(frame remote.ByteBuffer) error {
	f, ok := frame.(*unaryFrameBuffer)
	if capture, wrapped := frame.(*captureFrameBuffer); wrapped {
		f, ok = &capture.unaryFrameBuffer, true
	}
	if !ok || f.buffer == nil {
		return errors.New("invalid unary frame ownership")
	}
	if f.data != nil {
		if err := f.buffer.MallocAck(f.used); err != nil {
			return err
		}
	}
	if err := b.writer.Append(f.buffer); err != nil {
		return err
	}
	f.buffer = nil
	f.data = nil
	return nil
}

func (f *unaryFrameBuffer) ReserveFrame(n int) error {
	if f.data != nil || f.used != 0 || n < 0 || n > 16*1024*1024 {
		return errors.New("invalid unary frame reservation")
	}
	var err error
	f.data, err = f.buffer.Malloc(n)
	return err
}

func (f *unaryFrameBuffer) Malloc(n int) ([]byte, error) {
	if f.data == nil {
		p, err := f.buffer.Malloc(n)
		if err == nil {
			f.used += n
		}
		return p, err
	}
	if n < 0 || n > len(f.data)-f.used {
		return nil, errors.New("unary frame exceeds reservation")
	}
	p := f.data[f.used : f.used+n]
	f.used += n
	return p, nil
}

func (f *unaryFrameBuffer) WriteBinary(p []byte) (int, error) {
	if f.data == nil {
		n, err := f.buffer.WriteBinary(p)
		f.used += n
		return n, err
	}
	out, err := f.Malloc(len(p))
	if err != nil {
		return 0, err
	}
	return copy(out, p), nil
}
func (f *unaryFrameBuffer) Write(p []byte) (int, error) { return f.WriteBinary(p) }
func (f *unaryFrameBuffer) WriteString(s string) (int, error) {
	if f.data == nil {
		n, err := f.buffer.WriteString(s)
		f.used += n
		return n, err
	}
	out, err := f.Malloc(len(s))
	if err != nil {
		return 0, err
	}
	return copy(out, s), nil
}
func (f *unaryFrameBuffer) WrittenLen() int { return f.used }
func (f *unaryFrameBuffer) Bytes() ([]byte, error) {
	if f.data != nil {
		return f.data[:f.used], nil
	}
	if err := f.buffer.Flush(); err != nil {
		return nil, err
	}
	return f.buffer.Bytes(), nil
}
func (f *unaryFrameBuffer) Flush() error { return nil }
func (f *unaryFrameBuffer) Release(error) error {
	if f.buffer != nil {
		_ = f.buffer.Close()
	}
	f.buffer = nil
	f.data = nil
	f.used = 0
	unaryFrames.Put(f)
	return nil
}

// CaptureSegments exposes private encoding nodes before they are transferred
// to the connection. Generic writers need neither a contiguous frame nor a copy.
func (f *unaryFrameBuffer) CaptureSegments(dst [][]byte) ([][]byte, error) {
	if f.data != nil {
		return append(dst, f.data[:f.used]), nil
	}
	if err := f.buffer.Flush(); err != nil {
		return dst, err
	}
	return f.buffer.PeekSlices(f.used, dst), nil
}

// Native recording observes immutable bytes, so its private encoder can retain
// the connection writer's normal NocopyWrite behavior. Plugin frame buffers
// keep their existing contiguous view and mutation contract.
type captureFrameBuffer struct{ unaryFrameBuffer }

var captureFrames = sync.Pool{New: func() any { return &captureFrameBuffer{} }}

func (b *netpollByteBuffer) NewCaptureFrameBuffer() remote.ByteBuffer {
	f := captureFrames.Get().(*captureFrameBuffer)
	f.buffer = netpoll.NewLinkBuffer()
	return f
}
func (f *captureFrameBuffer) ReserveFrame(int) error { return nil }
func (f *captureFrameBuffer) WriteDirect(p []byte, remain int) error {
	return f.buffer.WriteDirect(p, remain)
}
func (f *captureFrameBuffer) MallocAck(n int) error { f.used = n; return f.buffer.MallocAck(n) }
func (f *captureFrameBuffer) Release(error) error {
	if f.buffer != nil {
		_ = f.buffer.Close()
	}
	f.buffer = nil
	f.data = nil
	f.used = 0
	captureFrames.Put(f)
	return nil
}
