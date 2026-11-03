package netpoll

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cloudwego/netpoll"
)

func TestUnaryFrameTransfersBackingBuffer(t *testing.T) {
	wire := netpoll.NewLinkBuffer()
	out := NewWriterByteBuffer(wire).(*netpollByteBuffer)
	owner := out.NewFrameBuffer().(*frameBuffer)
	if err := owner.ReserveFrame(32768); err != nil {
		t.Fatal(err)
	}
	frame, _ := owner.Malloc(16384)
	copy(frame, bytes.Repeat([]byte{0x7a}, len(frame)))
	if wire.MallocLen() != 0 || wire.Len() != 0 {
		t.Fatal("frame attached before plugin acceptance")
	}
	if err := out.CommitFrame(owner); err != nil {
		t.Fatal(err)
	}
	owner.Release(nil)
	// A later allocation/reuse must not reclaim the transferred backing buffer.
	other := out.NewFrameBuffer().(*frameBuffer)
	other.ReserveFrame(32768)
	p, _ := other.Malloc(16384)
	copy(p, bytes.Repeat([]byte{0x55}, len(p)))
	other.Release(nil)
	if err := wire.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := wire.Peek(16384)
	if err != nil {
		t.Fatal(err)
	}
	if &got[0] != &frame[0] || !bytes.Equal(got, bytes.Repeat([]byte{0x7a}, len(got))) {
		t.Fatal("frame copied or reclaimed after transfer")
	}
	wire.Close()
	out.Release(nil)
}

func TestUnaryFrameDiscardPreservesPendingOutput(t *testing.T) {
	wire := netpoll.NewLinkBuffer()
	old, _ := wire.Malloc(3)
	copy(old, "old")
	wire.Flush()
	out := NewWriterByteBuffer(wire).(*netpollByteBuffer)
	owner := out.NewFrameBuffer().(*frameBuffer)
	owner.ReserveFrame(64)
	p, _ := owner.Malloc(3)
	copy(p, "new")
	owner.Release(nil)
	got, err := wire.Peek(3)
	if err != nil || string(got) != "old" || wire.Len() != 3 {
		t.Fatalf("discard damaged previous output: %q %v", got, err)
	}
	wire.Close()
	out.Release(nil)
}

type blockedFrameWriter struct{ *netpoll.LinkBuffer }

func (b *blockedFrameWriter) Flush() error { b.LinkBuffer.Flush(); return errors.New("write timeout") }

func TestUnaryFrameSurvivesFlushFailure(t *testing.T) {
	wire := &blockedFrameWriter{netpoll.NewLinkBuffer()}
	out := NewWriterByteBuffer(wire).(*netpollByteBuffer)
	owner := out.NewFrameBuffer().(*frameBuffer)
	owner.ReserveFrame(64)
	frame, _ := owner.Malloc(7)
	copy(frame, "pending")
	if err := out.CommitFrame(owner); err != nil {
		t.Fatal(err)
	}
	if out.Flush() == nil {
		t.Fatal("expected timeout")
	}
	owner.Release(nil)
	got, err := wire.Peek(7)
	if err != nil || string(got) != "pending" || &got[0] != &frame[0] {
		t.Fatalf("Flush failure lost buffer ownership: %q %v", got, err)
	}
	wire.Close()
	out.Release(nil)
}
