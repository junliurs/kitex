package trans

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/remote"
)

func TestFrameWriterStablePatchesAndReuse(t *testing.T) {
	b := &frameWriter{}
	payload := bytes.Repeat([]byte{0x8a}, 16384)
	for round := 0; round != 3; round++ {
		header, _ := b.Malloc(4)
		body, _ := b.Malloc(len(payload))
		copy(body, payload)
		binary.BigEndian.PutUint32(header, uint32(len(payload)))
		p, err := b.Bytes()
		if err != nil || binary.BigEndian.Uint32(p) != uint32(len(payload)) || !bytes.Equal(p[4:], payload) {
			t.Fatalf("patched frame lost after growth/reuse: round=%d err=%v", round, err)
		}
		if round > 0 && &p[0] != &header[0] {
			t.Fatal("warm encoding still aggregates into a second frame")
		}
		b.Release(nil)
	}
	// The old frame cannot be returned to the pool while its consumer borrows it.
	a := newFrameWriter()
	input, _ := a.Malloc(8)
	copy(input, "borrowed")
	view, _ := a.Bytes()
	other := newFrameWriter()
	if a == other {
		t.Fatal("live frame owner was reused")
	}
	if string(view) != "borrowed" {
		t.Fatal("borrowed frame changed")
	}
	other.Release(nil)
	a.Release(nil)
}

func TestFrameWriterDropsRareLargeBuffer(t *testing.T) {
	b := &frameWriter{}
	b.Malloc(1024 * 1024)
	b.Bytes()
	b.Release(nil)
	if cap(b.storage) != 0 {
		t.Fatal("rare large frame retained")
	}
}

func TestCapturedHeadersSurviveTransportAndFrameOverwrite(t *testing.T) {
	const header = "owned-header"
	const payload = "owned-payload"
	raw := make([]byte, 4+len(header)+len(payload))
	binary.BigEndian.PutUint32(raw, uint32(len(raw)-4))
	copy(raw[4:], header+payload)
	capture := &metaCapture{ByteBuffer: remote.NewReaderBuffer(raw), prefix: make([]byte, 0, 256)}
	metadata, err := capture.Next(4 + len(header))
	if err != nil {
		t.Fatal(err)
	}
	if &metadata[0] == &raw[0] {
		t.Fatal("decoded metadata still aliases transport")
	}
	frame, err := capture.frame(len(payload))
	if err != nil {
		t.Fatal(err)
	}
	if &frame[0] == &metadata[0] {
		t.Fatal("frame can overwrite metadata backing")
	}
	copy(frame, bytes.Repeat([]byte{'F'}, len(frame)))
	if err = capture.ByteBuffer.Skip(len(payload)); err != nil {
		t.Fatal(err)
	}
	capture.ByteBuffer.Release(nil)
	for i := range raw {
		raw[i] = 'X'
	}
	if string(metadata[4:]) != header {
		t.Fatal("retained metadata changed after buffer reuse")
	}
}

type ownedReadSource struct {
	remote.ByteBuffer
	raw       []byte
	available int
	releases  int
}

func (s *ownedReadSource) ReadableLen() int { return s.available - s.ByteBuffer.ReadLen() }
func (s *ownedReadSource) Peek(n int) ([]byte, error) {
	if n > s.ReadableLen() {
		panic("borrow waited for unavailable payload")
	}
	return s.ByteBuffer.Peek(n)
}

func (s *ownedReadSource) Release(e error) error {
	s.releases++
	for i := range s.raw {
		s.raw[i] = 'X'
	}
	return s.ByteBuffer.Release(e)
}

func borrowedTestSource() *ownedReadSource {
	raw := make([]byte, 32)
	binary.BigEndian.PutUint32(raw, uint32(len(raw)-4))
	binary.BigEndian.PutUint16(raw[4:], 0x1000)
	copy(raw[6:], "owned-metadataBODY")
	return &ownedReadSource{ByteBuffer: remote.NewReaderBuffer(raw), raw: raw, available: len(raw)}
}

func TestBorrowedReadKeepsTransportUntilPayloadCompletes(t *testing.T) {
	source := borrowedTestSource()
	capture := newMetaCapture(source)
	if capture.owner != source {
		t.Fatal("complete frame was copied")
	}
	metadata, err := capture.Next(20)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := capture.frame(12)
	if err != nil || &frame[0] != &source.raw[0] {
		t.Fatal("native input did not borrow original frame", err)
	}
	want := append([]byte(nil), metadata...)
	capture.payload = true
	body, err := capture.Next(12)
	if err != nil || len(body) != 12 || source.releases != 0 {
		t.Fatal("transport released before payload decode", err)
	}
	if len(capture.prefix) != 20 {
		t.Fatal("payload was appended to metadata")
	}
	capture.Release(nil)
	if source.releases != 1 || !bytes.Equal(metadata, want) {
		t.Fatal("metadata lost ownership or transport not released once")
	}
}

func TestIncompleteReadDoesNotWaitBeforeAdmission(t *testing.T) {
	source := borrowedTestSource()
	source.available = 20
	capture := newMetaCapture(source)
	if capture.owner != nil || source.ReadLen() != 0 {
		t.Fatal("incomplete frame consumed business bytes")
	}
	metadata, _ := capture.Next(20)
	want := append([]byte(nil), metadata...)
	source.available = len(source.raw)
	frame, err := capture.frame(12)
	if err != nil || &frame[0] == &source.raw[0] {
		t.Fatal("incomplete frame failed to use owned fallback", err)
	}
	capture.payload = true
	capture.Next(12)
	capture.Release(nil)
	if !bytes.Equal(metadata, want) || frame[4] == 'X' || source.releases != 1 {
		t.Fatal("fallback frame or header lost ownership")
	}
}

func TestInvalidReadLengthNeverBorrows(t *testing.T) {
	for _, length := range []uint32{0, 0xffffffff, 16 * 1024 * 1024} {
		source := borrowedTestSource()
		binary.BigEndian.PutUint32(source.raw, length)
		capture := newMetaCapture(source)
		if capture.owner != nil || source.ReadLen() != 0 {
			t.Fatal("invalid length consumed input", length)
		}
		capture.Release(nil)
	}
}

type fragmentedReadSource struct {
	remote.ByteBuffer
	reader *netpoll.LinkBuffer
	read   int
}

func (s *fragmentedReadSource) Next(n int) ([]byte, error) {
	p, e := s.reader.Next(n)
	if e == nil {
		s.read += n
	}
	return p, e
}
func (s *fragmentedReadSource) Peek(n int) ([]byte, error) { return s.reader.Peek(n) }
func (s *fragmentedReadSource) ReadableLen() int           { return s.reader.Len() }
func (s *fragmentedReadSource) ReadLen() int               { return s.read }
func (s *fragmentedReadSource) Release(error) error        { return s.reader.Close() }
func TestBorrowedFragmentedFrameSurvivesSubsequentReads(t *testing.T) {
	raw := make([]byte, 16384)
	binary.BigEndian.PutUint32(raw, uint32(len(raw)-4))
	binary.BigEndian.PutUint16(raw[4:], 0x1000)
	for i := 6; i < len(raw); i++ {
		raw[i] = byte(i)
	}
	link := netpoll.NewLinkBuffer()
	link.WriteBinary(raw[:8000])
	link.WriteBinary(raw[8000:])
	link.Flush()
	capture := newMetaCapture(&fragmentedReadSource{reader: link})
	if capture.owner == nil {
		t.Fatal("fragmented complete frame not borrowed")
	}
	capture.Next(40)
	// Repeated peeks must not reuse a transport Peek cache underneath the frame.
	capture.Peek(8001)
	capture.Peek(10000)
	frame, err := capture.frame(len(raw) - 40)
	if err != nil || !bytes.Equal(frame, raw) {
		t.Fatal("fragmented native input changed", err)
	}
	capture.payload = true
	capture.Next(len(raw) - 40)
	capture.Release(nil)
}

// Header admission must own stable storage without waiting for the body. The
// four-byte framed length consumed after TTHeader must fit the same allocation.
func TestCapturedTTHeaderStorageStableWithoutBody(t *testing.T) {
	source := borrowedTestSource()
	binary.BigEndian.PutUint16(source.raw[12:], 2) // header ends at 22
	source.available = 22
	capture := newMetaCapture(source)
	if source.ReadLen() != 0 || capture.owner != nil {
		t.Fatal("header reservation consumed input")
	}
	first, err := capture.Next(14)
	if err != nil {
		t.Fatal(err)
	}
	capture.Next(8)
	source.available = len(source.raw)
	capture.Next(4)
	if &first[0] != &capture.prefix[0] {
		t.Fatal("header capture moved while decoding metadata")
	}
	if &first[0] == &source.raw[0] {
		t.Fatal("metadata aliases transport")
	}
	want := append([]byte(nil), first...)
	capture.Release(nil)
	if !bytes.Equal(first, want) {
		t.Fatal("metadata changed after transport release")
	}
}

func TestCapturedTTHeaderReservationBounds(t *testing.T) {
	for _, available := range []int{6, 13, 17, 21} {
		source := borrowedTestSource()
		binary.BigEndian.PutUint16(source.raw[12:], 2)
		source.available = available
		capture := newMetaCapture(source)
		if cap(capture.prefix) != 0 || source.ReadLen() != 0 {
			t.Fatal("incomplete header reserved or consumed input", available)
		}
		capture.Release(nil)
	}
	for _, words := range []uint16{0, 65535} {
		source := borrowedTestSource()
		binary.BigEndian.PutUint16(source.raw[12:], words)
		capture := newMetaCapture(source)
		if cap(capture.prefix) != 0 {
			t.Fatal("invalid header size reserved storage", words)
		}
		capture.Release(nil)
	}
}

func TestCapturedChangedResultOwnsBytesBeforeNativeAndTransportRelease(t *testing.T) {
	for _, borrowed := range []bool{false, true} {
		source := borrowedTestSource()
		capture := &metaCapture{ByteBuffer: source}
		if borrowed {
			capture = newMetaCapture(source)
		}
		header, err := capture.Next(20)
		if err != nil {
			t.Fatal(err)
		}
		wantHeader := append([]byte(nil), header...)
		input, err := capture.frame(12)
		if err != nil {
			t.Fatal(err)
		}
		native := []byte("short changed native response")
		final := capture.retainResult(remote.UnaryBytesResult{Bytes: native, Lease: 9}, input)
		want := append([]byte(nil), native...)
		for i := range native {
			native[i] = 'N'
		}
		if err = capture.discardPayload(12); err != nil {
			t.Fatal(err)
		}
		capture.Release(nil)
		if !bytes.Equal(final, want) || !bytes.Equal(header, wantHeader) {
			t.Fatal("replacement/metadata aliases released native or transport storage", borrowed)
		}
	}
}
