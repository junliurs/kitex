package trans

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/kitex/pkg/remote"
)

func bytesLifecycle(ctx context.Context) (*remote.UnaryCall, remote.UnaryBytesLifecycle) {
	c := remote.UnaryCallFromContext(ctx)
	if c == nil {
		return nil, nil
	}
	h, _ := c.Lifecycle.(remote.UnaryBytesLifecycle)
	if enabled, ok := h.(interface{ BytesEnabled() bool }); ok && !enabled.BytesEnabled() {
		return c, nil
	}
	return c, h
}

func processBytes(ctx context.Context, c *remote.UnaryCall, h remote.UnaryBytesLifecycle, stage remote.UnaryBytesStage, input []byte) (remote.UnaryBytesResult, error) {
	r, err := h.ProcessBytes(ctx, c, stage, input)
	if err != nil {
		return remote.UnaryBytesResult{}, err
	}
	if len(r.Bytes) == 0 || r.Action > remote.UnaryBytesReject {
		releaseBytes(r)
		return remote.UnaryBytesResult{}, errors.New("invalid byte result")
	}
	return r, nil
}

func releaseBytes(r remote.UnaryBytesResult) {
	if r.Lease != 0 && r.ValueOwner != nil {
		_ = r.ValueOwner.ReleaseBytesValue(r.Lease)
	}
}
func copyBytesResult(r remote.UnaryBytesResult) []byte { return append([]byte(nil), r.Bytes...) }
func (b *metaCapture) retainResult(r remote.UnaryBytesResult, input []byte) []byte {
	if b.owner != nil {
		return copyBytesResult(r)
	}
	if len(r.Bytes) == len(input) && len(input) > 0 && &r.Bytes[0] == &input[0] {
		return input
	}
	return append(input[:0], r.Bytes...)
}
func (b *metaCapture) discardPayload(n int) error { return b.ByteBuffer.Skip(n) }
func writeFinalBytes(out remote.ByteBuffer, frame []byte) error {
	data, err := out.Malloc(len(frame))
	if err != nil {
		return err
	}
	copy(data, frame)
	return nil
}

var sentFrames, transferredFrames, copiedSendFrames atomic.Uint64

func UnaryRebuildFrameStats() map[string]uint64 {
	return map[string]uint64{"sent": sentFrames.Load(), "transferred": transferredFrames.Load(), "send_copied": copiedSendFrames.Load(), "borrowed": borrowedReadFrames.Load(), "receive_copied": copiedReadFrames.Load()}
}

type unaryFrameTransport interface {
	NewUnaryFrameBuffer() remote.ByteBuffer
	CommitUnaryFrame(remote.ByteBuffer) error
}

func encodeTransportBytes(ctx context.Context, codec remote.Codec, msg remote.Message, out remote.ByteBuffer) ([]byte, remote.ByteBuffer, error) {
	if t, ok := out.(unaryFrameTransport); ok {
		if _, sized := msg.Data().(interface{ BLength() int }); sized {
			b := t.NewUnaryFrameBuffer()
			if err := codec.Encode(ctx, msg, b); err != nil {
				b.Release(err)
				return nil, nil, err
			}
			frame, err := b.Bytes()
			if err != nil {
				b.Release(err)
				return nil, nil, err
			}
			sentFrames.Add(1)
			return frame, b, nil
		}
	}
	frame, b, err := encodeBytes(ctx, codec, msg)
	if err == nil {
		sentFrames.Add(1)
	}
	return frame, b, err
}

func writeTransportBytes(out, owner remote.ByteBuffer, input, frame []byte) error {
	if t, ok := out.(unaryFrameTransport); ok && len(input) == len(frame) && len(input) > 0 && &input[0] == &frame[0] {
		if _, private := owner.(interface{ ReserveFrame(int) error }); private {
			if err := t.CommitUnaryFrame(owner); err != nil {
				return err
			}
			transferredFrames.Add(1)
			return nil
		}
	}
	p, err := out.Malloc(len(frame))
	if err != nil {
		return err
	}
	copy(p, frame)
	copiedSendFrames.Add(1)
	return nil
}

func encodeBytes(ctx context.Context, codec remote.Codec, msg remote.Message) ([]byte, *frameWriter, error) {
	b := newFrameWriter()
	if err := codec.Encode(ctx, msg, b); err != nil {
		b.Release(err)
		return nil, nil, err
	}
	p, err := b.Bytes()
	if err != nil {
		b.Release(err)
		return nil, nil, err
	}
	return p, b, nil
}

// Codec.Encode patches length fields through previously returned Malloc views.
// Each allocation must remain at its original address until encoding finishes.
type frameWriter struct {
	remote.ByteBuffer
	chunks    [][]byte
	size      int
	storage   []byte
	used      int
	scattered bool
	pooled    bool
}

var frameWriters = sync.Pool{New: func() any { return &frameWriter{} }}

func newFrameWriter() *frameWriter {
	b := frameWriters.Get().(*frameWriter)
	b.pooled = true
	return b
}

func (b *frameWriter) Malloc(n int) ([]byte, error) {
	var p []byte
	if n <= cap(b.storage)-b.used {
		p = b.storage[b.used : b.used+n]
		b.used += n
	} else {
		// Never move an earlier Malloc view while the codec patches lengths.
		p = make([]byte, n)
		b.scattered = true
	}
	b.chunks = append(b.chunks, p)
	b.size += n
	return p, nil
}
func (b *frameWriter) WrittenLen() int { return b.size }
func (b *frameWriter) WriteBinary(p []byte) (int, error) {
	v, _ := b.Malloc(len(p))
	copy(v, p)
	return len(p), nil
}
func (b *frameWriter) Write(p []byte) (int, error) { return b.WriteBinary(p) }
func (b *frameWriter) WriteString(s string) (int, error) {
	p, _ := b.Malloc(len(s))
	copy(p, s)
	return len(s), nil
}

func (b *frameWriter) Bytes() ([]byte, error) {
	if !b.scattered {
		return b.storage[:b.size], nil
	}
	p := make([]byte, b.size)
	offset := 0
	for _, chunk := range b.chunks {
		offset += copy(p[offset:], chunk)
	}
	b.storage = p
	return p, nil
}
func (b *frameWriter) Flush() error { return nil }
func (b *frameWriter) Release(error) error {
	for i := range b.chunks {
		b.chunks[i] = nil
	}
	b.chunks = b.chunks[:0]
	b.size, b.used, b.scattered = 0, 0, false
	// Reuse actual working storage, with a bound for rare large requests.
	if cap(b.storage) > 64*1024 {
		b.storage = nil
	}
	if b.pooled {
		b.pooled = false
		frameWriters.Put(b)
	}
	return nil
}
func (b *frameWriter) NewBuffer() remote.ByteBuffer { return newFrameWriter() }
func (b *frameWriter) AppendBuffer(other remote.ByteBuffer) error {
	p, err := other.Bytes()
	if err != nil {
		return err
	}
	_, err = b.WriteBinary(p)
	return err
}

// Remember only the bytes consumed by DecodeMeta. We retain early header
// admission: no extra full-body read occurs until governance has allowed it.
type metaCapture struct {
	remote.ByteBuffer
	prefix   []byte
	borrowed []byte
	owner    remote.ByteBuffer
	payload  bool
}

var borrowedReadFrames, copiedReadFrames atomic.Uint64

// UnaryBytesReadStats reports native input ownership at the two receive stages.
func UnaryBytesReadStats() (uint64, uint64) {
	return borrowedReadFrames.Load(), copiedReadFrames.Load()
}

func newMetaCapture(reader remote.ByteBuffer) *metaCapture {
	b := &metaCapture{ByteBuffer: reader}
	// Never wait for business data before header admission. Next provides stable
	// storage until Release; a multi-node Peek cache could be reused by DecodeMeta.
	available := reader.ReadableLen()
	if available < 6 {
		return b
	}
	peek := 6
	if available >= 14 {
		peek = 14
	}
	head, err := reader.Peek(peek)
	if err != nil || binary.BigEndian.Uint16(head[4:]) != 0x1000 {
		return b
	}
	size := uint64(binary.BigEndian.Uint32(head)) + 4
	if size < 18 || size > 16*1024*1024 {
		return b
	}
	if len(head) >= 14 {
		headerEnd := 14 + int(binary.BigEndian.Uint16(head[12:]))*4
		if headerEnd >= 18 && uint64(headerEnd) <= size && headerEnd <= available {
			// DecodeMeta owns its header bytes after transport release. Reserve
			// once for that header plus the optional framed payload length;
			// never reserve business data or wait for an incomplete header.
			capacity := headerEnd + 4
			if uint64(capacity) > size {
				capacity = int(size)
			}
			b.prefix = make([]byte, 0, capacity)
		}
	}
	if size > uint64(available) {
		return b
	}
	raw, err := reader.Next(int(size))
	if err != nil {
		return b
	}
	b.borrowed, b.owner = raw, reader
	b.ByteBuffer = remote.NewReaderBuffer(raw)
	return b
}

func (b *metaCapture) Release(err error) error {
	e := b.ByteBuffer.Release(err)
	if b.owner != nil {
		if ownerErr := b.owner.Release(err); e == nil {
			e = ownerErr
		}
		b.owner = nil
	}
	b.borrowed = nil
	return e
}

func (b *metaCapture) Next(n int) ([]byte, error) {
	if b.payload {
		return b.ByteBuffer.Next(n)
	}
	v, err := b.ByteBuffer.Next(n)
	if err != nil {
		return nil, err
	}
	start := len(b.prefix)
	b.prefix = append(b.prefix, v...)
	return b.prefix[start:], nil
}

func (b *metaCapture) Skip(n int) error {
	if b.payload {
		return b.ByteBuffer.Skip(n)
	}
	v, err := b.ByteBuffer.Peek(n)
	if err != nil {
		return err
	}
	b.prefix = append(b.prefix, v...)
	return b.ByteBuffer.Skip(n)
}

func (b *metaCapture) ReadString(n int) (string, error) {
	if b.payload {
		return b.ByteBuffer.ReadString(n)
	}
	v, err := b.ByteBuffer.Peek(n)
	if err != nil {
		return "", err
	}
	start := len(b.prefix)
	b.prefix = append(b.prefix, v...)
	if err = b.ByteBuffer.Skip(n); err != nil {
		return "", err
	}
	return string(b.prefix[start:]), nil
}

func (b *metaCapture) ReadBinary(dst []byte) (int, error) {
	if b.payload {
		return b.ByteBuffer.ReadBinary(dst)
	}
	n, err := b.ByteBuffer.ReadBinary(dst)
	if n > 0 {
		b.prefix = append(b.prefix, dst[:n]...)
	}
	return n, err
}

func (b *metaCapture) Read(dst []byte) (int, error) {
	if b.payload {
		return b.ByteBuffer.Read(dst)
	}
	n, err := b.ByteBuffer.Read(dst)
	if n > 0 {
		b.prefix = append(b.prefix, dst[:n]...)
	}
	return n, err
}

func (b *metaCapture) frame(n int) ([]byte, error) {
	if b.borrowed != nil {
		if n < 0 || len(b.prefix)+n != len(b.borrowed) {
			return nil, errors.New("borrowed frame length mismatch")
		}
		borrowedReadFrames.Add(1)
		return b.borrowed, nil
	}
	v, err := b.ByteBuffer.Peek(n)
	if err != nil {
		return nil, err
	}
	// Header fields may retain the prefix. A replacement must never overwrite it.
	raw := append(b.prefix[:len(b.prefix):len(b.prefix)], v...)
	if len(raw) < 4 || int(binary.BigEndian.Uint32(raw))+4 != len(raw) {
		return nil, errors.New("captured frame length mismatch")
	}
	copiedReadFrames.Add(1)
	return raw, nil
}

func responseWire(raw []byte) remote.UnaryWire {
	w := remote.UnaryWire{Framed: true}
	if len(raw) >= 16 {
		w.SeqID = int32(binary.BigEndian.Uint32(raw[8:]))
		w.Flags = binary.BigEndian.Uint16(raw[6:])
		w.ProtocolID = uint16(raw[14])
		end := 14 + int(binary.BigEndian.Uint16(raw[12:]))*4
		if end+8 <= len(raw) {
			w.MessageType = binary.BigEndian.Uint32(raw[end+4:]) & 255
		}
	}
	return w
}

func (b *metaCapture) ConsumedLen() int { return remote.ConsumedLen(b.ByteBuffer) }
