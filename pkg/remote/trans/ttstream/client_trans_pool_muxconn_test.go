//go:build !windows

/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ttstream

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	mocknetpoll "github.com/cloudwego/kitex/internal/mocks/netpoll"
	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/remote/trans/ttstream/container"
	"github.com/cloudwego/kitex/pkg/streaming"
)

func newTestMuxTransport(t *testing.T, p transPool, addr net.Addr) *transport {
	ctrl := gomock.NewController(t)
	conn := mocknetpoll.NewMockConnection(ctrl)
	conn.EXPECT().LocalAddr().Return(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}).AnyTimes()
	conn.EXPECT().RemoteAddr().Return(addr).AnyTimes()
	conn.EXPECT().IsActive().Return(true).AnyTimes()
	return &transport{
		kind:          clientTransport,
		conn:          conn,
		pool:          p,
		streams:       sync.Map{},
		spipe:         container.NewPipe[*stream](),
		scache:        make([]*stream, 0, streamCacheSize),
		fpipe:         container.NewPipe[*Frame](),
		closedTrigger: make(chan struct{}, 2),
	}
}

func waitForTransportClosed(t *testing.T, trans *transport, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&trans.closedFlag) == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("idle cleanup should close transport")
}

func TestMuxConnTransPool_IdleCleanupClosesTransport(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{
		PoolSize:       1,
		MaxIdleTimeout: time.Millisecond,
	}).(*muxConnTransPool)
	defer p.Close()

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8888}
	trans := newTestMuxTransport(t, p, addr)
	tl := newMuxConnTransList(1, p)
	tl.transports[0] = trans
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())
	p.pool.Store(addr.String(), tl)

	// start the cleaner goroutine, then force the list back to idle age
	p.Put(trans)
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())

	waitForTransportClosed(t, trans, time.Second)
}

func TestMuxConnTransPool_ActiveStreamPreventsIdleClose(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{
		PoolSize:       1,
		MaxIdleTimeout: time.Millisecond,
	}).(*muxConnTransPool)
	defer p.Close()

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8889}
	trans := newTestMuxTransport(t, p, addr)
	test.Assert(t, trans.storeStream(&stream{streamFrame: streamFrame{sid: genStreamID()}}) == nil)
	tl := newMuxConnTransList(1, p)
	tl.transports[0] = trans
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())
	p.pool.Store(addr.String(), tl)

	p.Put(trans)
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())

	// cleaner ticks every millisecond; give it several cycles to prove no close
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&trans.closedFlag) == 1 {
		t.Fatal("idle cleanup must not close transport with an active stream")
	}

	// once the active stream finishes, the next idle cycle closes it
	test.Assert(t, trans.CloseStream(genStreamID()) == nil)
	if atomic.LoadInt32(&trans.activeStreams) != 1 {
		t.Fatal("unmatched stream id should not change active stream count")
	}
	trans.streams.Range(func(key, value any) bool {
		test.Assert(t, trans.CloseStream(key.(int32)) == nil)
		return true
	})
	waitForTransportClosed(t, trans, time.Second)
}

func TestMuxConnTransPool_CheckedOutTransportPreventsIdleClose(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{
		PoolSize:       1,
		MaxIdleTimeout: time.Millisecond,
	}).(*muxConnTransPool)
	defer p.Close()

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8890}
	trans := newTestMuxTransport(t, p, addr)
	tl := newMuxConnTransList(1, p)
	tl.transports[0] = trans
	p.pool.Store(addr.String(), tl)

	got, err := p.Get(addr.Network(), addr.String())
	test.Assert(t, err == nil, err)
	test.Assert(t, got == trans)
	test.Assert(t, atomic.LoadInt32(&trans.pendingStreams) == 1)

	// Start the real cleaner and make the list old enough to be collected.
	// The lease acquired by Get must keep it alive until NewStream finishes.
	p.Put(trans)
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&trans.closedFlag) == 1 {
		t.Fatal("idle cleanup must not close a checked-out transport")
	}

	s := newStream(context.Background(), trans, streamFrame{sid: genStreamID(), method: "Bidi"})
	test.Assert(t, trans.WriteStream(context.Background(), s, make(IntHeader), make(streaming.Header)) == nil)
	p.Release(trans)
	test.Assert(t, atomic.LoadInt32(&trans.pendingStreams) == 0)
	test.Assert(t, atomic.LoadInt32(&trans.activeStreams) == 1)

	// Once the lease becomes an active stream, that stream continues to keep
	// the transport alive. Only closing it makes the list collectible.
	atomic.StoreInt64(&tl.lastUsed, time.Now().Add(-time.Hour).UnixNano())
	time.Sleep(50 * time.Millisecond)
	if atomic.LoadInt32(&trans.closedFlag) == 1 {
		t.Fatal("idle cleanup must not close a transport with an active stream")
	}
	test.Assert(t, trans.CloseStream(s.sid) == nil)
	waitForTransportClosed(t, trans, time.Second)
}

func TestTransport_WriteStreamAfterCloseReturnsError(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{PoolSize: 1}).(*muxConnTransPool)
	defer p.Close()

	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8891}
	trans := newTestMuxTransport(t, p, addr)
	test.Assert(t, trans.Close(nil) == nil)

	s := newStream(context.Background(), trans, streamFrame{sid: genStreamID(), method: "Bidi"})
	err := trans.WriteStream(context.Background(), s, make(IntHeader), make(streaming.Header))
	if !errors.Is(err, errTransport) {
		t.Fatalf("expected errTransport, got %v", err)
	}
	if _, ok := trans.loadStream(s.sid); ok {
		t.Fatal("failed WriteStream must not register the stream")
	}
	test.Assert(t, atomic.LoadInt32(&trans.activeStreams) == 0)
}

func TestMuxConnTransPool_GetAfterCloseReturnsError(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{
		PoolSize:       1,
		MaxIdleTimeout: time.Millisecond,
	}).(*muxConnTransPool)

	p.Close()

	_, err := p.Get("tcp", "127.0.0.1:8888")
	if !errors.Is(err, errMuxPoolClosed) {
		t.Fatalf("expected errMuxPoolClosed, got %v", err)
	}
}
