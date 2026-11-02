package trans

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/cloudwego/gopkg/protocol/ttheader"

	"github.com/cloudwego/kitex/pkg/remote"
)

type unaryCaptureObserver struct {
	remote.UnaryLifecycle
	enabled, panicOnFrame bool
	frames                [][]byte
}

func (o *unaryCaptureObserver) CaptureRequested(*remote.UnaryCall) bool { return o.enabled }
func (o *unaryCaptureObserver) CaptureFrame(_ context.Context, _ *remote.UnaryCall, _ remote.UnaryBytesStage, p []byte) {
	if o.panicOnFrame {
		panic("recording failed")
	}
	o.frames = append(o.frames, append([]byte(nil), p...))
}

func TestUnaryCaptureDoesNotEnableBytePluginsOrObserveUnselectedCall(t *testing.T) {
	o := &unaryCaptureObserver{}
	c := remote.NewUnaryCall(o)
	ctx := remote.WithUnaryCall(context.Background(), c)
	if _, h := captureLifecycle(ctx); h != nil {
		t.Fatal("disabled capture observer active")
	}
	if _, h := bytesLifecycle(ctx); h != nil {
		t.Fatal("capture enabled a byte plugin")
	}
	o.enabled = true
	if call, h := captureLifecycle(ctx); call != c || h != o {
		t.Fatal("selected observer missing")
	}
}

func TestUnaryCaptureRebuildsReceivedHeadersAndKeepsPayload(t *testing.T) {
	for _, framed := range []bool{false, true} {
		o := &unaryCaptureObserver{enabled: true}
		c := remote.NewUnaryCall(o)
		ctx := remote.WithUnaryCall(context.Background(), c)
		msg := remote.NewMessage(nil, newMockRPCInfo(), remote.Call, remote.Server)
		remote.AccessHeaders(msg.TransInfo()).SetString("capture-extra", "before\x00after")
		payload := []byte{0x80, 1, 0, 1, 0, 0, 0, 4, 'E', 'c', 'h', 'o', 0, 0, 0, 7, 0}
		captureReceivedPayload(ctx, msg, c, o, remote.UnaryBytesIngressRequest, payload, remote.UnaryWire{WithTTHeader: true, SeqID: 7, Framed: framed})
		if len(o.frames) != 1 {
			t.Fatal("missing capture")
		}
		f := o.frames[0]
		if int(binary.BigEndian.Uint32(f))+4 != len(f) {
			t.Fatal("frame length")
		}
		header, err := ttheader.DecodeFromBytes(context.Background(), f)
		if err != nil || header.StrInfo["capture-extra"] != "before\x00after" {
			t.Fatalf("headers %+v %v", header, err)
		}
		offset := 14 + int(binary.BigEndian.Uint16(f[12:]))*4
		if framed {
			if int(binary.BigEndian.Uint32(f[offset:])) != len(payload) {
				t.Fatal("inner length")
			}
			offset += 4
		}
		if !bytes.Equal(f[offset:], payload) {
			t.Fatal("capture changed business payload")
		}
	}
}

func TestUnaryCaptureObserverPanicDoesNotEscape(t *testing.T) {
	o := &unaryCaptureObserver{enabled: true, panicOnFrame: true}
	c := remote.NewUnaryCall(o)
	captureWire(context.Background(), c, o, remote.UnaryBytesEgressRequest, []byte("business"))
}

func TestUnaryCaptureFramedKeepsActualWireWithoutSyntheticHeaders(t *testing.T) {
	o := &unaryCaptureObserver{enabled: true}
	c := remote.NewUnaryCall(o)
	defer c.Recycle()
	msg := remote.NewMessage(nil, newMockRPCInfo(), remote.Call, remote.Server)
	defer remote.RecycleMessage(msg)
	remote.AccessHeaders(msg.TransInfo()).SetString("logical-only", "must not become TTHeader")
	payload := []byte{0x80, 1, 0, 1, 0, 0, 0, 4, 'E', 'c', 'h', 'o', 0, 0, 0, 7, 0}
	captureReceivedPayload(context.Background(), msg, c, o, remote.UnaryBytesIngressRequest, payload, remote.UnaryWire{Framed: true, SeqID: 7})
	expected := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(expected, uint32(len(payload)))
	copy(expected[4:], payload)
	if len(o.frames) != 1 || !bytes.Equal(o.frames[0], expected) {
		t.Fatal("capture changed the actual Framed wire", o.frames)
	}
}
