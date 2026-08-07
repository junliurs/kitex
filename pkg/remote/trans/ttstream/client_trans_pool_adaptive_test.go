//go:build !windows

package ttstream

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAdaptiveConnTransPoolReusesIdleTransport(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverConnection := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			serverConnection <- connection
		}
	}()

	pool := newAdaptiveConnTransPool(AdaptiveConnConfig{
		MaxConnections: 4,
		MaxIdleTimeout: time.Minute,
	}).(*adaptiveConnTransPool)
	addr := listener.Addr().String()
	trans, err := pool.Get("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	connection := <-serverConnection
	defer connection.Close()

	pool.Put(trans)
	reused, err := pool.Get("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	if reused != trans {
		t.Fatal("adaptive pool did not reuse the idle transport")
	}
	active := atomic.LoadInt32(&trans.poolStreams)
	if active != 1 {
		t.Fatalf("active streams=%d, want 1", active)
	}
	pool.Put(reused)
	if active := atomic.LoadInt32(&trans.poolStreams); active != 0 {
		t.Fatalf("active streams after put=%d, want 0", active)
	}
	pool.Put(reused)
	if active := atomic.LoadInt32(&trans.poolStreams); active != 0 {
		t.Fatalf("active streams after duplicate put=%d, want 0", active)
	}
	_ = reused.Close(nil)
}

func TestAdaptiveConnTransPoolSharesAtLimit(t *testing.T) {
	const maxConnections = 4
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var accepted sync.WaitGroup
	accepted.Add(maxConnections)
	serverConnections := make(chan net.Conn, maxConnections)
	go func() {
		for {
			connection, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			serverConnections <- connection
			accepted.Done()
		}
	}()

	pool := newAdaptiveConnTransPool(AdaptiveConnConfig{
		MaxConnections: maxConnections,
		MaxIdleTimeout: time.Minute,
	}).(*adaptiveConnTransPool)
	const streams = 16
	results := make(chan *clientTransport, streams)
	errorsChannel := make(chan error, streams)
	var group sync.WaitGroup
	for index := 0; index < streams; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			trans, getErr := pool.Get("tcp", listener.Addr().String())
			if getErr != nil {
				errorsChannel <- getErr
				return
			}
			results <- trans
		}()
	}
	group.Wait()
	close(results)
	close(errorsChannel)
	for getErr := range errorsChannel {
		t.Fatal(getErr)
	}
	waitDone := make(chan struct{})
	go func() {
		accepted.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("server did not accept the bounded connection count")
	}

	unique := make(map[*clientTransport]struct{})
	for trans := range results {
		unique[trans] = struct{}{}
	}
	if len(unique) != maxConnections {
		t.Fatalf(
			"unique transports=%d, want %d",
			len(unique),
			maxConnections,
		)
	}
	value, ok := pool.pools.Load(listener.Addr().String())
	if !ok {
		t.Fatal("adaptive list is missing")
	}
	list := value.(*adaptiveConnTransList)
	list.mu.Lock()
	connectionCount := len(list.transports)
	activeCount := 0
	for _, trans := range list.transports {
		activeCount += int(atomic.LoadInt32(&trans.poolStreams))
	}
	list.mu.Unlock()
	if connectionCount != maxConnections {
		t.Fatalf(
			"connection count=%d, want %d",
			connectionCount,
			maxConnections,
		)
	}
	if activeCount != streams {
		t.Fatalf("active stream count=%d, want %d", activeCount, streams)
	}

	for trans := range unique {
		for {
			active := atomic.LoadInt32(&trans.poolStreams)
			if active == 0 {
				break
			}
			pool.Put(trans)
		}
		_ = trans.Close(nil)
	}
	for index := 0; index < maxConnections; index++ {
		connection := <-serverConnections
		_ = connection.Close()
	}
}

func TestAdaptiveConnTransPoolLimitsEachAddressIndependently(t *testing.T) {
	const maxConnections = 2
	listeners := make([]net.Listener, 0, 2)
	serverConnections := make(chan net.Conn, maxConnections*2)
	for index := 0; index < 2; index++ {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners = append(listeners, listener)
		go func(listener net.Listener) {
			for {
				connection, acceptErr := listener.Accept()
				if acceptErr != nil {
					return
				}
				serverConnections <- connection
			}
		}(listener)
	}
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()

	pool := newAdaptiveConnTransPool(AdaptiveConnConfig{
		MaxConnections: maxConnections,
		MaxIdleTimeout: time.Minute,
	}).(*adaptiveConnTransPool)
	acquired := make([]*clientTransport, 0, 6)
	unique := make(map[*clientTransport]struct{})
	for _, listener := range listeners {
		addr := listener.Addr().String()
		for index := 0; index < 3; index++ {
			trans, err := pool.Get("tcp", addr)
			if err != nil {
				t.Fatal(err)
			}
			acquired = append(acquired, trans)
			unique[trans] = struct{}{}
		}
		value, ok := pool.pools.Load(addr)
		if !ok {
			t.Fatalf("adaptive list is missing for %s", addr)
		}
		list := value.(*adaptiveConnTransList)
		list.mu.Lock()
		count := len(list.transports)
		list.mu.Unlock()
		if count != maxConnections {
			t.Fatalf(
				"address %s connection count=%d, want %d",
				addr,
				count,
				maxConnections,
			)
		}
	}
	if len(unique) != maxConnections*len(listeners) {
		t.Fatalf(
			"total unique transports=%d, want %d",
			len(unique),
			maxConnections*len(listeners),
		)
	}
	for _, trans := range acquired {
		pool.Put(trans)
	}
	for trans := range unique {
		_ = trans.Close(nil)
	}
	for index := 0; index < len(unique); index++ {
		connection := <-serverConnections
		_ = connection.Close()
	}
}

func TestAdaptiveConnTransPoolDefaultLimit(t *testing.T) {
	pool := newAdaptiveConnTransPool(AdaptiveConnConfig{}).(*adaptiveConnTransPool)
	if pool.config.MaxConnections != DefaultAdaptiveConnConfig.MaxConnections {
		t.Fatalf(
			"max connections=%d, want %d",
			pool.config.MaxConnections,
			DefaultAdaptiveConnConfig.MaxConnections,
		)
	}
	if pool.config.MaxIdleTimeout != DefaultAdaptiveConnConfig.MaxIdleTimeout {
		t.Fatalf(
			"idle timeout=%s, want %s",
			pool.config.MaxIdleTimeout,
			DefaultAdaptiveConnConfig.MaxIdleTimeout,
		)
	}
}

func TestReleaseAdaptivePoolStreamIsIdempotent(t *testing.T) {
	trans := new(clientTransport)
	atomic.StoreInt32(&trans.poolStreams, 1)
	const callers = 32
	results := make(chan bool, callers)
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- releaseAdaptivePoolStream(trans)
		}()
	}
	group.Wait()
	close(results)

	becameIdle := 0
	for result := range results {
		if result {
			becameIdle++
		}
	}
	if becameIdle != 1 {
		t.Fatalf("idle transitions=%d, want 1", becameIdle)
	}
	if active := atomic.LoadInt32(&trans.poolStreams); active != 0 {
		t.Fatalf("active streams=%d, want 0", active)
	}
}
