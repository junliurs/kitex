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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	mocknetpoll "github.com/cloudwego/kitex/internal/mocks/netpoll"
	"github.com/cloudwego/kitex/pkg/remote/trans/ttstream/container"
)

func TestMuxConnTransPool_IdleCleanupClosesTransport(t *testing.T) {
	p := newMuxConnTransPool(MuxConnConfig{
		PoolSize:       1,
		MaxIdleTimeout: time.Millisecond,
	}).(*muxConnTransPool)
	defer p.Close()

	ctrl := gomock.NewController(t)
	conn := mocknetpoll.NewMockConnection(ctrl)
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8888}
	conn.EXPECT().LocalAddr().Return(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9999}).AnyTimes()
	conn.EXPECT().RemoteAddr().Return(addr).AnyTimes()
	trans := &transport{
		kind:          clientTransport,
		conn:          conn,
		pool:          p,
		streams:       sync.Map{},
		spipe:         container.NewPipe[*stream](),
		scache:        make([]*stream, 0, streamCacheSize),
		fpipe:         container.NewPipe[*Frame](),
		closedTrigger: make(chan struct{}, 2),
	}
	tl := newMuxConnTransList(1, p)
	tl.transports[0] = trans
	p.pool.Store(addr.String(), tl)
	p.activity.Store(addr.String(), time.Now().Add(-time.Hour))
	p.Put(trans)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&trans.closedFlag) == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("idle cleanup should close removed transport")
}
