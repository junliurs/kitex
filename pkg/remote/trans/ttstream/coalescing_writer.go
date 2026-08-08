package ttstream

import (
	"context"
	"runtime"
	"sync"

	"github.com/cloudwego/gopkg/bufiox"
	"github.com/cloudwego/netpoll"
)

const (
	maxCoalescedFrames           = 16
	maxCoalescedPayloadBytes     = 256 * 1024
	maxPendingFrames             = maxCoalescedFrames - 1
	maxImmediateFlushBytes       = 4 * 1024
	defaultMinGroupCommitStreams = 3
	defaultGroupCommitYields     = 1
	defaultGroupCommitPending    = 1
	serverMidGroupCommitStreams  = 4
	serverMidGroupCommitYields   = 4
	serverMidGroupCommitPending  = 2
	serverHighGroupCommitStreams = 5
	serverHighGroupCommitYields  = 8
	serverHighGroupCommitPending = 3
)

type frameWriteResult struct {
	err    error
	leader bool
}

type pendingFrameWrite struct {
	frame *Frame
	done  chan frameWriteResult
}

var pendingFrameWritePool = sync.Pool{
	New: func() any {
		return &pendingFrameWrite{
			done: make(chan frameWriteResult, 1),
		}
	},
}

func acquirePendingFrameWrite(frame *Frame) *pendingFrameWrite {
	request := pendingFrameWritePool.Get().(*pendingFrameWrite)
	request.frame = frame
	return request
}

func releasePendingFrameWrite(request *pendingFrameWrite) {
	request.frame = nil
	pendingFrameWritePool.Put(request)
}

type coalescingWriter struct {
	mu   sync.Mutex
	cond *sync.Cond

	writer                 bufiox.Writer
	activeStreamCount      func() int32
	groupCommitYield       func()
	minGroupCommitStreams  int32
	maxGroupCommitYields   int
	groupCommitPending     int
	midGroupCommitStreams  int32
	midGroupCommitYields   int
	midGroupCommitPending  int
	highGroupCommitStreams int32
	highGroupCommitYields  int
	highGroupCommitPending int

	state     int32
	closing   bool
	closedErr error
	writing   bool

	pending      []*pendingFrameWrite
	pendingBytes int
	pendingPeak  int
	bytesPeak    int
	waiters      int
}

func newCoalescingWriter(
	writer bufiox.Writer,
	activeStreamCounts ...func() int32,
) *coalescingWriter {
	return newCoalescingWriterWithYields(
		writer,
		defaultMinGroupCommitStreams,
		defaultGroupCommitYields,
		defaultGroupCommitPending,
		activeStreamCounts...,
	)
}

func newCoalescingWriterWithYields(
	writer bufiox.Writer,
	minGroupCommitStreams int32,
	maxGroupCommitYields int,
	groupCommitPending int,
	activeStreamCounts ...func() int32,
) *coalescingWriter {
	var activeStreamCount func() int32
	if len(activeStreamCounts) > 0 {
		activeStreamCount = activeStreamCounts[0]
	}
	if maxGroupCommitYields < 0 {
		maxGroupCommitYields = 0
	}
	if minGroupCommitStreams < 1 {
		minGroupCommitStreams = 1
	}
	if groupCommitPending < 1 {
		groupCommitPending = 1
	}
	if groupCommitPending > maxPendingFrames {
		groupCommitPending = maxPendingFrames
	}
	value := &coalescingWriter{
		writer:                writer,
		activeStreamCount:     activeStreamCount,
		groupCommitYield:      runtime.Gosched,
		minGroupCommitStreams: minGroupCommitStreams,
		maxGroupCommitYields:  maxGroupCommitYields,
		groupCommitPending:    groupCommitPending,
		pending:               make([]*pendingFrameWrite, 0, maxPendingFrames),
	}
	value.cond = sync.NewCond(&value.mu)
	return value
}

func newServerCoalescingWriter(
	writer bufiox.Writer,
	activeStreamCount func() int32,
) *coalescingWriter {
	value := newCoalescingWriter(writer, activeStreamCount)
	value.midGroupCommitStreams = serverMidGroupCommitStreams
	value.midGroupCommitYields = serverMidGroupCommitYields
	value.midGroupCommitPending = serverMidGroupCommitPending
	value.highGroupCommitStreams = serverHighGroupCommitStreams
	value.highGroupCommitYields = serverHighGroupCommitYields
	value.highGroupCommitPending = serverHighGroupCommitPending
	return value
}

func (w *coalescingWriter) WriteFrame(frame *Frame) (err error, closeOwner bool) {
	request := acquirePendingFrameWrite(frame)
	defer releasePendingFrameWrite(request)

	for {
		w.mu.Lock()
		if w.state == connStateClosed || w.closing {
			err = w.closedErr
			if err == nil {
				err = errTransport.newBuilder().withCause(
					netpoll.ErrConnClosed,
				)
			}
			w.mu.Unlock()
			return err, false
		}
		if !w.writing {
			w.writing = true
			w.mu.Unlock()
			return w.writeAsLeader(request)
		}
		if w.canEnqueue(frame) {
			w.pending = append(w.pending, request)
			w.pendingBytes += len(frame.payload)
			if len(w.pending) > w.pendingPeak {
				w.pendingPeak = len(w.pending)
			}
			if w.pendingBytes > w.bytesPeak {
				w.bytesPeak = w.pendingBytes
			}
			w.mu.Unlock()

			result := <-request.done
			if result.leader {
				return w.writeAsLeader(request)
			}
			return result.err, false
		}
		w.waiters++
		w.cond.Wait()
		w.waiters--
		w.mu.Unlock()
	}
}

func (w *coalescingWriter) canEnqueue(frame *Frame) bool {
	return len(w.pending) < maxPendingFrames &&
		w.pendingBytes+len(frame.payload) <= maxCoalescedPayloadBytes
}

func (w *coalescingWriter) writeAsLeader(
	leader *pendingFrameWrite,
) (err error, closeOwner bool) {
	batch := []*pendingFrameWrite{leader}
	payloadBytes := len(leader.frame.payload)

	err = EncodeFrame(context.Background(), w.writer, leader.frame)
	if err == nil {
		maxYields, targetPending := w.groupCommitLimits()
		for attempts := 0; attempts < maxYields &&
			w.shouldYieldForBatch(targetPending); attempts++ {
			w.groupCommitYield()
		}
		w.mu.Lock()
		for len(w.pending) > 0 && len(batch) < maxCoalescedFrames {
			request := w.pending[0]
			if payloadBytes+len(request.frame.payload) >
				maxCoalescedPayloadBytes {
				break
			}
			w.pending = w.pending[1:]
			w.pendingBytes -= len(request.frame.payload)
			batch = append(batch, request)
			payloadBytes += len(request.frame.payload)
		}
		w.cond.Broadcast()
		w.mu.Unlock()

		for _, request := range batch[1:] {
			if err = EncodeFrame(
				context.Background(),
				w.writer,
				request.frame,
			); err != nil {
				break
			}
		}
	}
	if err == nil {
		if err = w.writer.Flush(); err != nil {
			err = errTransport.newBuilder().withCause(err)
		}
	}
	if err == nil {
		for _, request := range batch {
			recycleFrame(request.frame)
		}
	}

	var nextLeader *pendingFrameWrite
	var failedPending []*pendingFrameWrite
	w.mu.Lock()
	if err != nil {
		if w.state != connStateClosed {
			w.state = connStateClosed
			w.closedErr = err
			closeOwner = !w.closing
		}
		w.writing = false
		failedPending = w.pending
		w.pending = w.pending[:0]
		w.pendingBytes = 0
	} else if w.state != connStateClosed &&
		len(w.pending) > 0 {
		nextLeader = w.pending[0]
		w.pending = w.pending[1:]
		w.pendingBytes -= len(nextLeader.frame.payload)
	} else {
		w.writing = false
	}
	w.cond.Broadcast()
	w.mu.Unlock()

	result := frameWriteResult{err: err}
	for _, request := range batch[1:] {
		request.done <- result
	}
	for _, request := range failedPending {
		request.done <- result
	}
	if nextLeader != nil {
		nextLeader.done <- frameWriteResult{leader: true}
	}
	return err, closeOwner
}

func (w *coalescingWriter) groupCommitLimits() (
	maxYields,
	targetPending int,
) {
	if w.activeStreamCount == nil {
		return 0, 0
	}
	activeStreams := w.activeStreamCount()
	if activeStreams < w.minGroupCommitStreams {
		return 0, 0
	}
	maxYields = w.maxGroupCommitYields
	targetPending = w.groupCommitPending
	if w.midGroupCommitStreams > 0 &&
		activeStreams >= w.midGroupCommitStreams {
		maxYields = w.midGroupCommitYields
		targetPending = w.midGroupCommitPending
	}
	if w.highGroupCommitStreams > 0 &&
		activeStreams >= w.highGroupCommitStreams {
		maxYields = w.highGroupCommitYields
		targetPending = w.highGroupCommitPending
	}
	if maximum := int(activeStreams) - 1; targetPending > maximum {
		targetPending = maximum
	}
	return maxYields, targetPending
}

func (w *coalescingWriter) shouldYieldForBatch(targetPending int) bool {
	if w.activeStreamCount == nil ||
		targetPending < 1 ||
		w.writer.WrittenLen() >= maxImmediateFlushBytes {
		return false
	}
	w.mu.Lock()
	pending := len(w.pending)
	w.mu.Unlock()
	return pending < targetPending
}

func (w *coalescingWriter) Close(err error) (closeOwner bool, closedErr error) {
	w.mu.Lock()
	if w.state == connStateClosed {
		closedErr = w.closedErr
		w.mu.Unlock()
		return false, closedErr
	}
	if w.closing {
		for w.state != connStateClosed {
			w.cond.Wait()
		}
		closedErr = w.closedErr
		w.mu.Unlock()
		return false, closedErr
	}

	w.closing = true
	w.closedErr = err
	w.cond.Broadcast()
	w.mu.Unlock()

	w.mu.Lock()
	for w.writing {
		w.cond.Wait()
	}
	if w.state != connStateClosed {
		w.state = connStateClosed
		w.closedErr = err
	}
	closeOwner = true
	w.closing = false
	closedErr = w.closedErr
	w.cond.Broadcast()
	w.mu.Unlock()
	return closeOwner, closedErr
}

func (w *coalescingWriter) IsClosed() bool {
	w.mu.Lock()
	closed := w.state == connStateClosed || w.closing
	w.mu.Unlock()
	return closed
}
