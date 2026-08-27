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
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/gofunc"
)

var DefaultMuxConnConfig = MuxConnConfig{
	PoolSize:       runtime.GOMAXPROCS(0),
	MaxIdleTimeout: time.Minute,
}

type MuxConnConfig struct {
	PoolSize       int
	MaxIdleTimeout time.Duration
}

var _ transPool = (*muxConnTransPool)(nil)

var (
	errMuxListClosed = errors.New("mux connection list is closed")
	errMuxPoolClosed = errors.New("mux connection pool is closed")
)

type muxConnTransList struct {
	L                     sync.RWMutex
	size                  int
	cursor                uint32
	transports            []*transport
	pool                  transPool
	closed                int32
	lastUsed              int64 // unix nano
	maxReceiveMessageSize int
}

func newMuxConnTransList(size int, pool transPool, maxReceiveMessageSize int) *muxConnTransList {
	tl := new(muxConnTransList)
	if size <= 0 {
		size = runtime.GOMAXPROCS(0)
	}
	tl.size = size
	tl.transports = make([]*transport, size)
	tl.pool = pool
	tl.maxReceiveMessageSize = maxReceiveMessageSize
	return tl
}

func (tl *muxConnTransList) Close() {
	if !atomic.CompareAndSwapInt32(&tl.closed, 0, 1) {
		return
	}
	tl.L.Lock()
	for i, t := range tl.transports {
		if t == nil {
			continue
		}
		_ = t.Close(nil)
		tl.transports[i] = nil
	}
	tl.L.Unlock()
}

// closeIfIdle removes and closes the list only when it has no in-flight streams
// and no transport has been checked out for creating a stream.
func (tl *muxConnTransList) closeIfIdle(now time.Time, idleTimeout time.Duration, removeFromPool func()) bool {
	tl.L.Lock()
	if atomic.LoadInt32(&tl.closed) == 1 {
		tl.L.Unlock()
		return false
	}
	if now.UnixNano()-atomic.LoadInt64(&tl.lastUsed) < int64(idleTimeout) {
		tl.L.Unlock()
		return false
	}
	for _, t := range tl.transports {
		if t == nil {
			continue
		}
		// Check the lease first. NewStream converts pending -> active in that
		// order, so the cleaner must observe at least one side of the handoff.
		if atomic.LoadInt32(&t.pendingStreams) > 0 || atomic.LoadInt32(&t.activeStreams) > 0 {
			tl.L.Unlock()
			return false
		}
	}
	atomic.StoreInt32(&tl.closed, 1)
	removeFromPool()
	for i, t := range tl.transports {
		if t == nil {
			continue
		}
		_ = t.Close(nil)
		tl.transports[i] = nil
	}
	tl.L.Unlock()
	return true
}

func (tl *muxConnTransList) Get(network, addr string) (*transport, error) {
	if atomic.LoadInt32(&tl.closed) == 1 {
		return nil, errMuxListClosed
	}
	// fast path
	idx := atomic.AddUint32(&tl.cursor, 1) % uint32(tl.size)
	tl.L.RLock()
	trans := tl.transports[idx]
	if trans != nil && trans.IsActive() {
		// Acquire a lease under RLock so closeIfIdle cannot close the transport
		// between Get returning and WriteStream registering the new stream.
		atomic.AddInt32(&trans.pendingStreams, 1)
		atomic.StoreInt64(&tl.lastUsed, time.Now().UnixNano())
		tl.L.RUnlock()
		return trans, nil
	}
	tl.L.RUnlock()
	if trans != nil {
		_ = trans.Close(nil)
	}

	// slow path
	tl.L.Lock()
	if atomic.LoadInt32(&tl.closed) == 1 {
		tl.L.Unlock()
		return nil, errMuxListClosed
	}
	trans = tl.transports[idx]
	if trans != nil && trans.IsActive() {
		// another goroutine already create the new transport
		atomic.AddInt32(&trans.pendingStreams, 1)
		atomic.StoreInt64(&tl.lastUsed, time.Now().UnixNano())
		tl.L.Unlock()
		return trans, nil
	}
	// it may create more than tl.size transport if multi client try to get transport concurrently
	conn, err := dialer.DialConnection(network, addr, time.Second)
	if err != nil {
		tl.L.Unlock()
		return nil, err
	}
	trans = newTransport(clientTransport, conn, tl.pool, withMaxReceiveMessageSize(tl.maxReceiveMessageSize))
	if atomic.LoadInt32(&tl.closed) == 1 {
		tl.L.Unlock()
		_ = trans.Close(nil)
		return nil, errMuxListClosed
	}
	_ = conn.AddCloseCallback(func(connection netpoll.Connection) error {
		// peer close
		_ = trans.Close(errTransport.WithCause(errors.New("connection closed by peer")))
		return nil
	})
	tl.transports[idx] = trans
	atomic.AddInt32(&trans.pendingStreams, 1)
	atomic.StoreInt64(&tl.lastUsed, time.Now().UnixNano())
	tl.L.Unlock()

	return trans, nil
}

func newMuxConnTransPool(config MuxConnConfig) transPool {
	t := new(muxConnTransPool)
	t.config = config
	t.closeCh = make(chan struct{})
	return t
}

type muxConnTransPool struct {
	config                MuxConnConfig
	maxReceiveMessageSize int
	pool                  sync.Map // addr:*muxConnTransList
	cleanerOnce           int32
	closed                int32
	closeCh               chan struct{}
}

func (p *muxConnTransPool) Get(network, addr string) (trans *transport, err error) {
	for {
		if atomic.LoadInt32(&p.closed) == 1 {
			return nil, errMuxPoolClosed
		}
		v, ok := p.pool.Load(addr)
		if !ok {
			// multi concurrent Get should get the same TransList object
			v, _ = p.pool.LoadOrStore(addr, newMuxConnTransList(p.config.PoolSize, p, p.maxReceiveMessageSize))
		}
		if atomic.LoadInt32(&p.closed) == 1 {
			return nil, errMuxPoolClosed
		}
		tl := v.(*muxConnTransList)
		trans, err = tl.Get(network, addr)
		if err != errMuxListClosed {
			return trans, err
		}
		if atomic.LoadInt32(&p.closed) == 1 {
			return nil, errMuxPoolClosed
		}
		// The idle cleaner removed this list from the pool; retry with a fresh list.
	}
}

func (p *muxConnTransPool) SetMaxReceiveMessageSize(size int) {
	p.maxReceiveMessageSize = size
}

func (p *muxConnTransPool) Put(trans *transport) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return
	}
	addr := trans.conn.RemoteAddr().String()
	if v, ok := p.pool.Load(addr); ok {
		atomic.StoreInt64(&v.(*muxConnTransList).lastUsed, time.Now().UnixNano())
	}

	idleTimeout := p.config.MaxIdleTimeout
	if idleTimeout == 0 {
		return
	}
	if !atomic.CompareAndSwapInt32(&p.cleanerOnce, 0, 1) {
		return
	}
	// start cleaning background goroutine
	gofunc.RecoverGoFuncWithInfo(context.Background(), func() {
		timer := time.NewTimer(idleTimeout)
		defer timer.Stop()
		for {
			select {
			case <-p.closeCh:
				return
			case <-timer.C:
			}
			if atomic.LoadInt32(&p.closed) == 1 {
				return
			}
			now := time.Now()
			p.pool.Range(func(key, value any) bool {
				addr := key.(string)
				tl := value.(*muxConnTransList)
				tl.closeIfIdle(now, idleTimeout, func() {
					p.pool.Delete(addr)
				})
				return true
			})
			timer.Reset(idleTimeout)
		}
	}, gofunc.NewBasicInfo("", trans.Addr().String()))
}

func (p *muxConnTransPool) Release(trans *transport) {
	atomic.AddInt32(&trans.pendingStreams, -1)
}

func (p *muxConnTransPool) Close() {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return
	}
	close(p.closeCh)
	p.pool.Range(func(key, value any) bool {
		value.(*muxConnTransList).Close()
		p.pool.Delete(key)
		return true
	})
}
