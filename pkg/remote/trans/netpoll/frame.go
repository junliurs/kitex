package netpoll

import (
	"errors"
	"sync"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/remote"
)

// A private frame is not attached to the connection until its producer commits
// it. Append transfers its nodes, so Flush timeout cannot invalidate ownership.
type frameBuffer struct {
	remote.ByteBuffer
	buffer *netpoll.LinkBuffer
	data   []byte
	used   int
}

var frames = sync.Pool{New: func() any { return &frameBuffer{} }}

func (b *netpollByteBuffer) NewFrameBuffer() remote.ByteBuffer {
	f := frames.Get().(*frameBuffer)
	f.buffer = netpoll.NewLinkBuffer()
	return f
}

func (b *netpollByteBuffer) CommitFrame(frame remote.ByteBuffer) error {
	f, ok := frame.(*frameBuffer)
	if capture, wrapped := frame.(*captureFrameBuffer); wrapped {
		f, ok = &capture.frameBuffer, true
	}
	if !ok || f.buffer == nil {
		return errors.New("invalid frame ownership")
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

func (f *frameBuffer) ReserveFrame(n int) error {
	if f.data != nil || f.used != 0 || n < 0 || n > 16*1024*1024 {
		return errors.New("invalid frame reservation")
	}
	var err error
	f.data, err = f.buffer.Malloc(n)
	return err
}

func (f *frameBuffer) Malloc(n int) ([]byte, error) {
	if f.data == nil {
		p, err := f.buffer.Malloc(n)
		if err == nil {
			f.used += n
		}
		return p, err
	}
	if n < 0 || n > len(f.data)-f.used {
		return nil, errors.New("frame exceeds reservation")
	}
	p := f.data[f.used : f.used+n]
	f.used += n
	return p, nil
}

func (f *frameBuffer) WriteBinary(p []byte) (int, error) {
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
func (f *frameBuffer) Write(p []byte) (int, error) { return f.WriteBinary(p) }
func (f *frameBuffer) WriteString(s string) (int, error) {
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
func (f *frameBuffer) WrittenLen() int { return f.used }
func (f *frameBuffer) Bytes() ([]byte, error) {
	if f.data != nil {
		return f.data[:f.used], nil
	}
	if err := f.buffer.Flush(); err != nil {
		return nil, err
	}
	return f.buffer.Bytes(), nil
}
func (f *frameBuffer) Flush() error { return nil }
func (f *frameBuffer) Release(error) error {
	if f.buffer != nil {
		_ = f.buffer.Close()
	}
	f.buffer = nil
	f.data = nil
	f.used = 0
	frames.Put(f)
	return nil
}

// CaptureSegments exposes private encoding nodes before they are transferred
// to the connection. Generic writers need neither a contiguous frame nor a copy.
func (f *frameBuffer) CaptureSegments(dst [][]byte) ([][]byte, error) {
	if f.data != nil {
		return append(dst, f.data[:f.used]), nil
	}
	if err := f.buffer.Flush(); err != nil {
		return dst, err
	}
	return f.buffer.PeekSlices(f.used, dst), nil
}

// Observers borrow immutable encoding nodes with the normal NocopyWrite
// behavior. A mutable private frame instead exposes a contiguous reserved view.
type captureFrameBuffer struct{ frameBuffer }

var captureFrames = sync.Pool{New: func() any { return &captureFrameBuffer{} }}

func (b *netpollByteBuffer) NewObservedFrameBuffer() remote.ByteBuffer {
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
