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
)

func newTestMuxTransport(t *testing.T, p transPool, addr net.Addr) *transport {
	ctrl := gomock.NewController(t)
	conn := mocknetpoll.NewMockConnection(ctrl)
	conn.EXPECT().LocalAddr().Return(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}).AnyTimes()
	conn.EXPECT().RemoteAddr().Return(addr).AnyTimes()
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
	trans.storeStream(&stream{streamFrame: streamFrame{sid: genStreamID()}})
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
