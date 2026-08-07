package ttstream

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/netpoll"

	"github.com/cloudwego/kitex/pkg/remote/trans/ttstream/internal/container"
)

var DefaultAdaptiveConnConfig = AdaptiveConnConfig{
	MaxConnections: runtime.GOMAXPROCS(0) * 2,
	MaxIdleTimeout: time.Minute,
}

type AdaptiveConnConfig struct {
	MaxConnections int
	MaxIdleTimeout time.Duration
}

type adaptiveConnTransPool struct {
	config AdaptiveConnConfig
	pools  sync.Map
	owners sync.Map
	idle   *container.ObjectPool
}

type adaptiveConnTransList struct {
	pool *adaptiveConnTransPool
	addr string

	mu         sync.Mutex
	cond       *sync.Cond
	transports []*clientTransport
	creating   int
	cursor     uint32
}

func newAdaptiveConnTransPool(config AdaptiveConnConfig) transPool {
	if config.MaxConnections <= 0 {
		config.MaxConnections = DefaultAdaptiveConnConfig.MaxConnections
	}
	if config.MaxIdleTimeout <= 0 {
		config.MaxIdleTimeout = DefaultAdaptiveConnConfig.MaxIdleTimeout
	}
	return &adaptiveConnTransPool{
		config: config,
		idle:   container.NewObjectPool(config.MaxIdleTimeout),
	}
}

func (p *adaptiveConnTransPool) Get(
	network,
	addr string,
) (*clientTransport, error) {
	candidate := &adaptiveConnTransList{
		pool: p,
		addr: addr,
	}
	candidate.cond = sync.NewCond(&candidate.mu)
	value, _ := p.pools.LoadOrStore(addr, candidate)
	return value.(*adaptiveConnTransList).Get(network, addr)
}

func (p *adaptiveConnTransPool) Put(trans *clientTransport) {
	value, ok := p.owners.Load(trans)
	if !ok {
		return
	}
	value.(*adaptiveConnTransList).Put(trans)
}

func (p *adaptiveConnTransPool) remove(trans *clientTransport) {
	value, ok := p.owners.LoadAndDelete(trans)
	if !ok {
		return
	}
	value.(*adaptiveConnTransList).Remove(trans)
}

func (l *adaptiveConnTransList) Get(
	network,
	addr string,
) (*clientTransport, error) {
	for {
		for {
			object := l.pool.idle.Pop(addr)
			if object == nil {
				break
			}
			trans := object.(*clientTransport)
			if trans.IsActive() &&
				atomic.CompareAndSwapInt32(&trans.poolStreams, 0, 1) {
				return trans, nil
			}
			if !trans.IsActive() {
				l.mu.Lock()
				l.removeLocked(trans)
				l.mu.Unlock()
			}
		}

		l.mu.Lock()
		if len(l.transports)+l.creating < l.pool.config.MaxConnections {
			l.creating++
			l.mu.Unlock()
			trans, err := l.create(network, addr)
			l.mu.Lock()
			l.creating--
			if err == nil && trans.IsActive() {
				l.transports = append(l.transports, trans)
				atomic.StoreInt32(&trans.poolStreams, 1)
			} else if err == nil {
				l.pool.owners.Delete(trans)
				err = errTransport.newBuilder().withCause(
					netpoll.ErrConnClosed,
				)
			}
			l.cond.Broadcast()
			l.mu.Unlock()
			if err != nil {
				return nil, err
			}
			return trans, nil
		}

		size := len(l.transports)
		if size == 0 && l.creating > 0 {
			l.cond.Wait()
			l.mu.Unlock()
			continue
		}
		for attempts := 0; attempts < size; attempts++ {
			index := int(atomic.AddUint32(&l.cursor, 1)) % size
			trans := l.transports[index]
			if trans.IsActive() {
				atomic.AddInt32(&trans.poolStreams, 1)
				l.mu.Unlock()
				return trans, nil
			}
			l.removeLocked(trans)
			size = len(l.transports)
			if size == 0 {
				break
			}
		}
		l.mu.Unlock()
	}
}

func (l *adaptiveConnTransList) Put(trans *clientTransport) {
	if !trans.IsActive() {
		return
	}
	if releaseAdaptivePoolStream(trans) {
		l.pool.idle.Push(l.addr, trans)
	}
}

func releaseAdaptivePoolStream(trans *clientTransport) bool {
	for {
		active := atomic.LoadInt32(&trans.poolStreams)
		if active <= 0 {
			return false
		}
		if atomic.CompareAndSwapInt32(
			&trans.poolStreams,
			active,
			active-1,
		) {
			return active == 1
		}
	}
}

func (l *adaptiveConnTransList) Remove(trans *clientTransport) {
	l.mu.Lock()
	l.removeLocked(trans)
	l.mu.Unlock()
}

func (l *adaptiveConnTransList) removeLocked(trans *clientTransport) {
	for index, candidate := range l.transports {
		if candidate != trans {
			continue
		}
		last := len(l.transports) - 1
		l.transports[index] = l.transports[last]
		l.transports = l.transports[:last]
		break
	}
}

func (l *adaptiveConnTransList) create(
	network,
	addr string,
) (*clientTransport, error) {
	conn, err := dialer.DialConnection(network, addr, time.Second)
	if err != nil {
		return nil, err
	}
	trans := newClientTransport(conn, l.pool)
	l.pool.owners.Store(trans, l)
	if err := conn.AddCloseCallback(func(netpoll.Connection) error {
		l.pool.remove(trans)
		_ = trans.Close(
			errTransport.newBuilder().withCause(
				errors.New("connection closed by peer"),
			),
		)
		return nil
	}); err != nil {
		l.pool.owners.Delete(trans)
		_ = trans.Close(err)
		return nil, err
	}
	return trans, nil
}
