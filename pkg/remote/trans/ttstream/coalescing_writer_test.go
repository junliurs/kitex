package ttstream

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/netpoll"
)

type blockingProfileWriter struct {
	mu sync.Mutex

	buffer      []byte
	flushes     int
	flushError  error
	firstMalloc chan struct{}
	release     chan struct{}
	blockOnce   sync.Once
}

func newBlockingProfileWriter() *blockingProfileWriter {
	return &blockingProfileWriter{
		firstMalloc: make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (w *blockingProfileWriter) Malloc(size int) ([]byte, error) {
	w.blockOnce.Do(func() {
		close(w.firstMalloc)
		<-w.release
	})
	w.mu.Lock()
	defer w.mu.Unlock()
	start := len(w.buffer)
	w.buffer = append(w.buffer, make([]byte, size)...)
	return w.buffer[start:], nil
}

func (w *blockingProfileWriter) WriteBinary(value []byte) (int, error) {
	w.mu.Lock()
	w.buffer = append(w.buffer, value...)
	w.mu.Unlock()
	return len(value), nil
}

func (w *blockingProfileWriter) WrittenLen() int {
	w.mu.Lock()
	length := len(w.buffer)
	w.mu.Unlock()
	return length
}

func (w *blockingProfileWriter) Flush() error {
	w.mu.Lock()
	w.flushes++
	w.buffer = w.buffer[:0]
	err := w.flushError
	w.mu.Unlock()
	return err
}

func TestCoalescingWriterFlushesConcurrentFramesTogether(t *testing.T) {
	const writers = 8
	writer := newBlockingProfileWriter()
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, writers)

	go writeCoalescingTestFrame(coalescer, 0, 1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < writers; index++ {
		go writeCoalescingTestFrame(coalescer, index, 1024, results)
	}
	waitForPendingFrames(t, coalescer, writers-1)
	close(writer.release)

	var closeOwners int
	for index := 0; index < writers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("write %d returned %v", index, result.err)
		}
		if result.leader {
			closeOwners++
		}
	}
	if closeOwners != 0 {
		t.Fatalf("close owners=%d, want 0", closeOwners)
	}
	if writer.flushes != 1 {
		t.Fatalf("flushes=%d, want 1", writer.flushes)
	}
	if coalescer.pendingPeak != writers-1 {
		t.Fatalf(
			"pending peak=%d, want %d",
			coalescer.pendingPeak,
			writers-1,
		)
	}
	if coalescer.bytesPeak != (writers-1)*1024 {
		t.Fatalf(
			"bytes peak=%d, want %d",
			coalescer.bytesPeak,
			(writers-1)*1024,
		)
	}
}

func TestCoalescingWriterYieldForSharedSmallBatch(t *testing.T) {
	writer := newBlockingProfileWriter()
	writer.buffer = make([]byte, maxImmediateFlushBytes-1)
	activeStreams := int32(defaultMinGroupCommitStreams)
	coalescer := newCoalescingWriter(
		writer,
		func() int32 {
			return atomic.LoadInt32(&activeStreams)
		},
	)
	maxYields, targetPending := coalescer.groupCommitLimits()
	if maxYields != defaultGroupCommitYields ||
		targetPending != defaultGroupCommitPending {
		t.Fatalf(
			"group commit limits=(%d,%d), want (%d,%d)",
			maxYields,
			targetPending,
			defaultGroupCommitYields,
			defaultGroupCommitPending,
		)
	}
	if !coalescer.shouldYieldForBatch(targetPending) {
		t.Fatal("shared small batch did not request group commit")
	}

	atomic.StoreInt32(&activeStreams, defaultMinGroupCommitStreams-1)
	maxYields, targetPending = coalescer.groupCommitLimits()
	if maxYields != 0 || targetPending != 0 {
		t.Fatalf(
			"below-threshold limits=(%d,%d), want (0,0)",
			maxYields,
			targetPending,
		)
	}
	if coalescer.shouldYieldForBatch(targetPending) {
		t.Fatal("connection below the group-commit threshold yielded")
	}

	atomic.StoreInt32(&activeStreams, defaultMinGroupCommitStreams)
	_, targetPending = coalescer.groupCommitLimits()
	writer.buffer = make([]byte, maxImmediateFlushBytes)
	if coalescer.shouldYieldForBatch(targetPending) {
		t.Fatal("large batch yielded")
	}

	writer.buffer = writer.buffer[:0]
	coalescer.pending = append(
		coalescer.pending,
		acquirePendingFrameWrite(newFrame(
			streamFrame{sid: 1, method: "Test"},
			dataFrameType,
			nil,
		)),
	)
	defer func() {
		request := coalescer.pending[0]
		coalescer.pending = coalescer.pending[:0]
		recycleFrame(request.frame)
		releasePendingFrameWrite(request)
	}()
	if coalescer.shouldYieldForBatch(targetPending) {
		t.Fatal("writer with an existing pending frame yielded")
	}

	withoutCounter := newCoalescingWriter(writer)
	_, targetPending = withoutCounter.groupCommitLimits()
	if withoutCounter.shouldYieldForBatch(targetPending) {
		t.Fatal("writer without an active-stream counter yielded")
	}
}

func TestServerCoalescingWriterRampsGroupCommit(t *testing.T) {
	writer := newBlockingProfileWriter()
	activeStreams := int32(0)
	coalescer := newServerCoalescingWriter(
		writer,
		func() int32 {
			return atomic.LoadInt32(&activeStreams)
		},
	)
	testCases := []struct {
		activeStreams int32
		maxYields     int
		targetPending int
	}{
		{activeStreams: defaultMinGroupCommitStreams - 1},
		{
			activeStreams: defaultMinGroupCommitStreams,
			maxYields:     defaultGroupCommitYields,
			targetPending: defaultGroupCommitPending,
		},
		{
			activeStreams: serverMidGroupCommitStreams,
			maxYields:     serverMidGroupCommitYields,
			targetPending: serverMidGroupCommitPending,
		},
		{
			activeStreams: serverHighGroupCommitStreams,
			maxYields:     serverHighGroupCommitYields,
			targetPending: serverHighGroupCommitPending,
		},
		{
			activeStreams: serverHighGroupCommitStreams + 3,
			maxYields:     serverHighGroupCommitYields,
			targetPending: serverHighGroupCommitPending,
		},
	}
	for _, testCase := range testCases {
		atomic.StoreInt32(&activeStreams, testCase.activeStreams)
		maxYields, targetPending := coalescer.groupCommitLimits()
		if maxYields != testCase.maxYields ||
			targetPending != testCase.targetPending {
			t.Fatalf(
				"active streams=%d limits=(%d,%d), want (%d,%d)",
				testCase.activeStreams,
				maxYields,
				targetPending,
				testCase.maxYields,
				testCase.targetPending,
			)
		}
	}
}

func TestCoalescingWriterBoundsGroupCommitYields(t *testing.T) {
	writer := newBlockingProfileWriter()
	close(writer.release)
	activeStreams := int32(defaultMinGroupCommitStreams)
	coalescer := newCoalescingWriter(
		writer,
		func() int32 {
			return atomic.LoadInt32(&activeStreams)
		},
	)
	yieldCount := 0
	coalescer.groupCommitYield = func() {
		yieldCount++
	}
	frame := newFrame(
		streamFrame{sid: 1, method: "Test"},
		dataFrameType,
		make([]byte, 1024),
	)
	err, closeOwner := coalescer.WriteFrame(frame)
	if err != nil || closeOwner {
		t.Fatalf("write result err=%v closeOwner=%t", err, closeOwner)
	}
	if yieldCount != defaultGroupCommitYields {
		t.Fatalf(
			"yield count=%d, want %d",
			yieldCount,
			defaultGroupCommitYields,
		)
	}

	serverCoalescer := newServerCoalescingWriter(
		writer,
		func() int32 {
			return atomic.LoadInt32(&activeStreams)
		},
	)
	atomic.StoreInt32(&activeStreams, defaultMinGroupCommitStreams)
	yieldCount = 0
	serverCoalescer.groupCommitYield = func() {
		yieldCount++
	}
	frame = newFrame(
		streamFrame{sid: 4, method: "Test"},
		dataFrameType,
		make([]byte, 1024),
	)
	err, closeOwner = serverCoalescer.WriteFrame(frame)
	if err != nil || closeOwner {
		t.Fatalf("server write result err=%v closeOwner=%t", err, closeOwner)
	}
	if yieldCount != defaultGroupCommitYields {
		t.Fatalf(
			"server yield count=%d, want %d",
			yieldCount,
			defaultGroupCommitYields,
		)
	}

	yieldCount = 0
	var pending *pendingFrameWrite
	serverCoalescer.groupCommitYield = func() {
		yieldCount++
		if yieldCount != 1 {
			return
		}
		pending = acquirePendingFrameWrite(newFrame(
			streamFrame{sid: 2, method: "Test"},
			dataFrameType,
			make([]byte, 1024),
		))
		serverCoalescer.mu.Lock()
		serverCoalescer.pending = append(serverCoalescer.pending, pending)
		serverCoalescer.pendingBytes += len(pending.frame.payload)
		serverCoalescer.mu.Unlock()
	}
	frame = newFrame(
		streamFrame{sid: 3, method: "Test"},
		dataFrameType,
		make([]byte, 1024),
	)
	err, closeOwner = serverCoalescer.WriteFrame(frame)
	if err != nil || closeOwner {
		t.Fatalf("batched write result err=%v closeOwner=%t", err, closeOwner)
	}
	if yieldCount != 1 {
		t.Fatalf("yield count with pending frame=%d, want 1", yieldCount)
	}
	result := <-pending.done
	if result.err != nil || result.leader {
		t.Fatalf("pending write result=%+v", result)
	}
	releasePendingFrameWrite(pending)

	serverWriter := newBlockingProfileWriter()
	close(serverWriter.release)
	atomic.StoreInt32(&activeStreams, serverHighGroupCommitStreams)
	serverCoalescer = newServerCoalescingWriter(
		serverWriter,
		func() int32 {
			return atomic.LoadInt32(&activeStreams)
		},
	)
	yieldCount = 0
	pendingWrites := make(
		[]*pendingFrameWrite,
		0,
		serverHighGroupCommitPending,
	)
	serverCoalescer.groupCommitYield = func() {
		yieldCount++
		request := acquirePendingFrameWrite(newFrame(
			streamFrame{
				sid:    int32(10 + yieldCount),
				method: "Test",
			},
			dataFrameType,
			make([]byte, 1024),
		))
		pendingWrites = append(pendingWrites, request)
		serverCoalescer.mu.Lock()
		serverCoalescer.pending = append(serverCoalescer.pending, request)
		serverCoalescer.pendingBytes += len(request.frame.payload)
		serverCoalescer.mu.Unlock()
	}
	frame = newFrame(
		streamFrame{sid: 20, method: "Test"},
		dataFrameType,
		make([]byte, 1024),
	)
	err, closeOwner = serverCoalescer.WriteFrame(frame)
	if err != nil || closeOwner {
		t.Fatalf(
			"server pending-target write err=%v closeOwner=%t",
			err,
			closeOwner,
		)
	}
	if yieldCount != serverHighGroupCommitPending {
		t.Fatalf(
			"server pending-target yields=%d, want %d",
			yieldCount,
			serverHighGroupCommitPending,
		)
	}
	if serverWriter.flushes != 1 {
		t.Fatalf("server pending-target flushes=%d, want 1", serverWriter.flushes)
	}
	for index, request := range pendingWrites {
		result := <-request.done
		if result.err != nil || result.leader {
			t.Fatalf("pending write %d result=%+v", index, result)
		}
		releasePendingFrameWrite(request)
	}
}

func TestCoalescingWriterBoundsPendingFrames(t *testing.T) {
	const writers = 64
	writer := newBlockingProfileWriter()
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, writers)

	go writeCoalescingTestFrame(coalescer, 0, 64*1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < writers; index++ {
		go writeCoalescingTestFrame(coalescer, index, 64*1024, results)
	}
	waitForPendingFrames(t, coalescer, 4)
	close(writer.release)

	for index := 0; index < writers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("write %d returned %v", index, result.err)
		}
	}
	if coalescer.pendingPeak > maxPendingFrames {
		t.Fatalf(
			"pending peak=%d, max=%d",
			coalescer.pendingPeak,
			maxPendingFrames,
		)
	}
	if coalescer.bytesPeak > maxCoalescedPayloadBytes {
		t.Fatalf(
			"bytes peak=%d, max=%d",
			coalescer.bytesPeak,
			maxCoalescedPayloadBytes,
		)
	}
	if writer.flushes < writers/maxCoalescedFrames {
		t.Fatalf(
			"flushes=%d, want at least %d",
			writer.flushes,
			writers/maxCoalescedFrames,
		)
	}
}

func TestCoalescingWriterBroadcastsFlushError(t *testing.T) {
	const writers = 8
	flushError := errors.New("flush failed")
	writer := newBlockingProfileWriter()
	writer.flushError = flushError
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, writers)

	go writeCoalescingTestFrame(coalescer, 0, 1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < writers; index++ {
		go writeCoalescingTestFrame(coalescer, index, 1024, results)
	}
	waitForPendingFrames(t, coalescer, writers-1)
	close(writer.release)

	var closeOwners int32
	for index := 0; index < writers; index++ {
		result := <-results
		if result.err == nil {
			t.Fatalf("write %d returned nil error", index)
		}
		if result.leader {
			atomic.AddInt32(&closeOwners, 1)
		}
	}
	if closeOwners != 1 {
		t.Fatalf("close owners=%d, want 1", closeOwners)
	}
	closeOwner, err := coalescer.Close(errors.New("later close"))
	if closeOwner {
		t.Fatal("closed coalescer returned another close owner")
	}
	if err == nil {
		t.Fatal("closed coalescer returned nil error")
	}
}

func TestCoalescingWriterCloseDrainsAcceptedFrames(t *testing.T) {
	const writers = 8
	writer := newBlockingProfileWriter()
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, writers)

	go writeCoalescingTestFrame(coalescer, 0, 1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < writers; index++ {
		go writeCoalescingTestFrame(coalescer, index, 1024, results)
	}
	waitForPendingFrames(t, coalescer, writers-1)

	closeResult := make(chan frameWriteResult, 1)
	go func() {
		closeOwner, err := coalescer.Close(nil)
		closeResult <- frameWriteResult{err: err, leader: closeOwner}
	}()
	select {
	case result := <-closeResult:
		t.Fatalf("close returned before accepted writes drained: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}

	close(writer.release)
	for index := 0; index < writers; index++ {
		result := <-results
		if result.err != nil {
			t.Fatalf("write %d returned %v", index, result.err)
		}
	}
	result := <-closeResult
	if result.err != nil {
		t.Fatalf("close returned %v", result.err)
	}
	if !result.leader {
		t.Fatal("close did not own resource release")
	}
	if writer.flushes != 1 {
		t.Fatalf("flushes=%d, want 1", writer.flushes)
	}
	if !coalescer.IsClosed() {
		t.Fatal("coalescer is not closed")
	}
}

func TestCoalescingWriterCloseReturnsConcurrentFlushError(t *testing.T) {
	flushError := errors.New("flush failed")
	writer := newBlockingProfileWriter()
	writer.flushError = flushError
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, 1)

	go writeCoalescingTestFrame(coalescer, 0, 1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}

	closeResult := make(chan frameWriteResult, 1)
	go func() {
		closeOwner, err := coalescer.Close(nil)
		closeResult <- frameWriteResult{err: err, leader: closeOwner}
	}()
	waitForClosing(t, coalescer)
	close(writer.release)

	writeResult := <-results
	if writeResult.err == nil {
		t.Fatal("write returned nil error")
	}
	result := <-closeResult
	if result.err == nil || !strings.Contains(
		result.err.Error(),
		flushError.Error(),
	) {
		t.Fatalf("close error=%v, want flush error", result.err)
	}
	closeOwners := 0
	if writeResult.leader {
		closeOwners++
	}
	if result.leader {
		closeOwners++
	}
	if closeOwners != 1 {
		t.Fatalf("close owners=%d, want exactly one", closeOwners)
	}
}

func TestCoalescingWriterCloseRejectsUnacceptedWrite(t *testing.T) {
	writer := newBlockingProfileWriter()
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, maxCoalescedFrames+1)

	go writeCoalescingTestFrame(coalescer, 0, 1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < maxCoalescedFrames; index++ {
		go writeCoalescingTestFrame(coalescer, index, 1024, results)
	}
	waitForPendingFrames(t, coalescer, maxPendingFrames)
	go writeCoalescingTestFrame(
		coalescer,
		maxCoalescedFrames,
		1024,
		results,
	)
	waitForBlockedWriters(t, coalescer, 1)

	closeResult := make(chan frameWriteResult, 1)
	go func() {
		closeOwner, err := coalescer.Close(nil)
		closeResult <- frameWriteResult{err: err, leader: closeOwner}
	}()
	waitForClosing(t, coalescer)
	close(writer.release)

	closedErrors := 0
	for index := 0; index < maxCoalescedFrames+1; index++ {
		result := <-results
		if result.err == nil {
			continue
		}
		if !errors.Is(result.err, netpoll.ErrConnClosed) {
			t.Fatalf("write error=%v, want netpoll.ErrConnClosed", result.err)
		}
		closedErrors++
	}
	if closedErrors != 1 {
		t.Fatalf("closed write errors=%d, want 1", closedErrors)
	}
	result := <-closeResult
	if result.err != nil {
		t.Fatalf("close returned %v", result.err)
	}
}

func TestCoalescingWriterSenderOOMCancelStormStaysBounded(t *testing.T) {
	const writers = 512
	writer := newBlockingProfileWriter()
	coalescer := newCoalescingWriter(writer)
	results := make(chan frameWriteResult, writers)

	go writeCoalescingTestFrame(coalescer, 0, 64*1024, results)
	select {
	case <-writer.firstMalloc:
	case <-time.After(time.Second):
		t.Fatal("leader did not start encoding")
	}
	for index := 1; index < writers; index++ {
		frameType := dataFrameType
		if index%4 == 0 {
			frameType = rstFrameType
		}
		go writeCoalescingTestTypedFrame(
			coalescer,
			index,
			frameType,
			64*1024,
			results,
		)
	}
	waitForPendingFrames(t, coalescer, 4)
	waitForBlockedWriters(t, coalescer, writers-maxCoalescedFrames)

	const closers = 32
	closeResults := make(chan frameWriteResult, closers)
	for index := 0; index < closers; index++ {
		go func() {
			closeOwner, err := coalescer.Close(nil)
			closeResults <- frameWriteResult{err: err, leader: closeOwner}
		}()
	}
	waitForClosing(t, coalescer)
	close(writer.release)

	closedWrites := 0
	for index := 0; index < writers; index++ {
		result := <-results
		if result.err == nil {
			continue
		}
		if !errors.Is(result.err, netpoll.ErrConnClosed) {
			t.Fatalf("write error=%v, want netpoll.ErrConnClosed", result.err)
		}
		closedWrites++
	}
	if closedWrites == 0 {
		t.Fatal("close storm did not reject any unaccepted writers")
	}
	closeOwners := 0
	for index := 0; index < closers; index++ {
		result := <-closeResults
		if result.err != nil {
			t.Fatalf("close returned %v", result.err)
		}
		if result.leader {
			closeOwners++
		}
	}
	if closeOwners != 1 {
		t.Fatalf("close owners=%d, want 1", closeOwners)
	}
	if coalescer.pendingPeak > maxPendingFrames {
		t.Fatalf(
			"pending peak=%d, max=%d",
			coalescer.pendingPeak,
			maxPendingFrames,
		)
	}
	if coalescer.bytesPeak > maxCoalescedPayloadBytes {
		t.Fatalf(
			"bytes peak=%d, max=%d",
			coalescer.bytesPeak,
			maxCoalescedPayloadBytes,
		)
	}
}

func writeCoalescingTestFrame(
	coalescer *coalescingWriter,
	index int,
	payloadSize int,
	results chan<- frameWriteResult,
) {
	writeCoalescingTestTypedFrame(
		coalescer,
		index,
		dataFrameType,
		payloadSize,
		results,
	)
}

func writeCoalescingTestTypedFrame(
	coalescer *coalescingWriter,
	index int,
	frameType int32,
	payloadSize int,
	results chan<- frameWriteResult,
) {
	frame := newFrame(
		streamFrame{sid: int32(index + 1), method: "Test"},
		frameType,
		make([]byte, payloadSize),
	)
	err, closeOwner := coalescer.WriteFrame(frame)
	results <- frameWriteResult{err: err, leader: closeOwner}
}

func waitForPendingFrames(
	t *testing.T,
	coalescer *coalescingWriter,
	minimum int,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		coalescer.mu.Lock()
		pending := len(coalescer.pending)
		coalescer.mu.Unlock()
		if pending >= minimum {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("pending frames did not reach %d", minimum)
}

func waitForBlockedWriters(
	t *testing.T,
	coalescer *coalescingWriter,
	minimum int,
) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		coalescer.mu.Lock()
		waiters := coalescer.waiters
		coalescer.mu.Unlock()
		if waiters >= minimum {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("blocked writers did not reach %d", minimum)
}

func waitForClosing(t *testing.T, coalescer *coalescingWriter) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		coalescer.mu.Lock()
		closing := coalescer.closing
		coalescer.mu.Unlock()
		if closing {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("coalescer did not enter closing state")
}
