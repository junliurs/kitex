package netpoll

import (
	"bytes"
	"testing"

	"github.com/cloudwego/netpoll"
)

func TestUnaryBorrowedMessageUsesConsumedRangeAndNoUnselectedAllocation(t *testing.T) {
	b := netpoll.NewLinkBuffer()
	defer b.Close()
	_, _ = b.WriteBinary([]byte("message-next"))
	_ = b.Flush()
	r := NewReaderByteBuffer(b).(*netpollByteBuffer)
	defer r.Release(nil)
	if n := testing.AllocsPerRun(100, func() { r.BeginBorrowedMessage() }); n != 0 {
		t.Fatalf("unselected message allocated %v", n)
	}
	if err := r.Skip(7); err != nil {
		t.Fatal(err)
	}
	var scratch [4][]byte
	parts := r.BorrowConsumedMessage(scratch[:0])
	if len(parts) != 1 || !bytes.Equal(parts[0], []byte("message")) || r.ReadLen() != 0 || r.ConsumedLen() != 7 {
		t.Fatal("wrong message boundary", parts, r.ReadLen())
	}
	view := parts[0]
	parts = r.BorrowConsumedMessage(scratch[:0])
	if &parts[0][0] != &view[0] {
		t.Fatal("recording created a payload copy")
	}
	remaining, err := r.Next(5)
	if err != nil || string(remaining) != "-next" {
		t.Fatal("recording consumed the following message", err)
	}
}

func TestUnaryCaptureEncoderRetainsDirectPayload(t *testing.T) {
	wire := netpoll.NewLinkBuffer()
	defer wire.Close()
	out := NewWriterByteBuffer(wire).(*netpollByteBuffer)
	defer out.Release(nil)
	owner := out.NewCaptureFrameBuffer().(*captureFrameBuffer)
	defer owner.Release(nil)
	payload := bytes.Repeat([]byte("payload"), 20000)
	buf, err := owner.Malloc(4 + len(payload))
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, "head")
	if err = owner.WriteDirect(payload, len(payload)); err != nil {
		t.Fatal(err)
	}
	if err = owner.MallocAck(4 + len(payload)); err != nil {
		t.Fatal(err)
	}
	var scratch [8][]byte
	parts, err := owner.CaptureSegments(scratch[:0])
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || string(parts[0]) != "head" || len(parts[1]) != len(payload) || &parts[1][0] != &payload[0] {
		t.Fatal("encoder copied or reordered direct payload", len(parts))
	}
	if err = out.CommitUnaryFrame(owner); err != nil {
		t.Fatal(err)
	}
	if err = out.Flush(); err != nil {
		t.Fatal(err)
	}
	if wire.Len() != 4+len(payload) {
		t.Fatal("wrong committed size", wire.Len())
	}
}
