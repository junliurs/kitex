package connpool

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	config "github.com/cloudwego/kitex/pkg/connpool"
	"github.com/cloudwego/kitex/pkg/remote"
)

// Count real pool ownership decisions without a socket, server, or timer race.
type retirementConn struct {
	address netAddr
	closed  atomic.Int32
}

func (c *retirementConn) Read([]byte) (int, error)       { return 0, io.EOF }
func (c *retirementConn) Write(p []byte) (int, error)    { return len(p), nil }
func (c *retirementConn) Close() error                   { c.closed.Add(1); return nil }
func (c *retirementConn) IsActive() bool                 { return c.closed.Load() == 0 }
func (c *retirementConn) LocalAddr() net.Addr            { return netAddr{"tcp", "127.0.0.1:1"} }
func (c *retirementConn) RemoteAddr() net.Addr           { return c.address }
func (*retirementConn) SetDeadline(time.Time) error      { return nil }
func (*retirementConn) SetReadDeadline(time.Time) error  { return nil }
func (*retirementConn) SetWriteDeadline(time.Time) error { return nil }

type retirementDialer struct {
	mu    sync.Mutex
	conns []*retirementConn
}

func (d *retirementDialer) DialTimeout(network, address string, _ time.Duration) (net.Conn, error) {
	c := &retirementConn{address: netAddr{network, address}}
	d.mu.Lock()
	d.conns = append(d.conns, c)
	d.mu.Unlock()
	return c, nil
}

func retirementPool(t *testing.T) (*LongPool, remote.ConnOption) {
	t.Helper()
	p := NewLongPool("meshlib.retirement", config.IdleConfig{MaxIdlePerAddress: 4, MaxIdleGlobal: 16, MaxIdleTimeout: time.Hour})
	t.Cleanup(func() { _ = p.Close() })
	return p, remote.ConnOption{Dialer: new(retirementDialer), ConnectTimeout: time.Second}
}

func retirementGet(t *testing.T, p *LongPool, opt remote.ConnOption) *longConn {
	t.Helper()
	c, err := p.Get(context.Background(), "tcp", "127.0.0.1:8099", opt)
	if err != nil {
		t.Fatal(err)
	}
	return c.(*longConn)
}

func TestMeshlibRetiredConnectionCannotEnterReplacementPeer(t *testing.T) {
	p, opt := retirementPool(t)
	old := retirementGet(t, p, opt)
	p.Clean("tcp", old.address)
	replacement := retirementGet(t, p, opt)
	if err := p.Put(old); err != nil {
		t.Fatal(err)
	}
	if got := old.Conn.(*retirementConn).closed.Load(); got != 1 {
		t.Fatalf("retired connection entered replacement peer; close count = %d", got)
	}
	if err := p.Put(replacement); err != nil {
		t.Fatal(err)
	}
	reused := retirementGet(t, p, opt)
	if reused != replacement {
		t.Fatal("replacement peer returned an old connection")
	}
	_ = p.Discard(reused)
}

func TestMeshlibLoadedPeerRejectsReturnAfterRetirement(t *testing.T) {
	p, opt := retirementPool(t)
	c := retirementGet(t, p, opt)
	loaded, _ := p.peerMap.Load(netAddr{"tcp", c.address})
	// Model Put already having loaded the original peer when Clean deletes it.
	p.Clean("tcp", c.address)
	loaded.(*peer).Close()
	if err := loaded.(*peer).Put(c); err != nil {
		t.Fatal(err)
	}
	if got := c.Conn.(*retirementConn).closed.Load(); got != 1 {
		t.Fatalf("return resurrected retired peer; close count = %d", got)
	}
	if got := loaded.(*peer).Len(); got != 0 {
		t.Fatalf("retired peer holds %d connections", got)
	}
	if got := p.globalIdle.Now(); got != 0 {
		t.Fatalf("retired return leaked %d idle permits", got)
	}
}

func TestMeshlibConcurrentRetirementDrainsConnections(t *testing.T) {
	p, opt := retirementPool(t)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 500; i++ {
				c, err := p.Get(context.Background(), "tcp", "127.0.0.1:8099", opt)
				if err != nil {
					t.Error(err)
					return
				}
				if i%3 == 0 {
					p.Clean("tcp", "127.0.0.1:8099")
				}
				if i%5 == 0 {
					p.CleanAll()
				}
				if err := p.Put(c); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	workers.Wait()
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := p.globalIdle.Now(); got != 0 {
		t.Fatalf("pool close leaked %d idle permits", got)
	}
	d := opt.Dialer.(*retirementDialer)
	for i, c := range d.conns {
		if got := c.closed.Load(); got != 1 {
			t.Errorf("connection %d closed %d times", i, got)
		}
	}
}

func TestMeshlibPoolResetKeepsBorrowedIOAndRejectsOldReturns(t *testing.T) {
	p, opt := retirementPool(t)
	held := retirementGet(t, p, opt)
	idle := retirementGet(t, p, opt)
	if err := p.Put(idle); err != nil {
		t.Fatal(err)
	}
	p.CleanAll()
	if held.Conn.(*retirementConn).closed.Load() != 0 {
		t.Fatal("reset interrupted borrowed I/O")
	}
	if idle.Conn.(*retirementConn).closed.Load() != 1 {
		t.Fatal("reset kept an old idle connection")
	}
	fresh := retirementGet(t, p, opt)
	if err := p.Put(held); err != nil {
		t.Fatal(err)
	}
	if held.Conn.(*retirementConn).closed.Load() != 1 {
		t.Fatal("old return entered new peer")
	}
	if err := p.Put(fresh); err != nil {
		t.Fatal(err)
	}
	p.CleanAll()
	if fresh.Conn.(*retirementConn).closed.Load() != 1 || p.globalIdle.Now() != 0 {
		t.Fatal("reset leaked idle state")
	}
}

func TestMeshlibConnectionPolicyBoundsAndExpiredActiveSocket(t *testing.T) {
	p, opt := retirementPool(t)
	policy := remote.ConnectionPolicy{Enabled: true, IdleTimeout: 10 * time.Millisecond, MaxIdleConnections: 1}
	get := func() *longConn {
		c, err := p.GetWithPolicy(context.Background(), "tcp", "127.0.0.1:8099", opt, policy)
		if err != nil {
			t.Fatal(err)
		}
		return c.(*longConn)
	}
	a, b := get(), get()
	_ = p.Put(a)
	_ = p.Put(b)
	if b.Conn.(*retirementConn).closed.Load() != 1 || p.globalIdle.Now() != 1 {
		t.Fatal("CP idle limit ignored")
	}
	time.Sleep(15 * time.Millisecond)
	fresh := get()
	if fresh == a || a.Conn.(*retirementConn).closed.Load() != 1 {
		t.Fatal("active Netpoll-like socket reused after CP idle deadline")
	}
	_ = p.Discard(fresh)
	policy.MaxIdleConnections = 0
	c := get()
	_ = p.Put(c)
	if c.Conn.(*retirementConn).closed.Load() != 1 || p.globalIdle.Now() != 0 {
		t.Fatal("zero CP idle limit retained a connection")
	}
}

func TestMeshlibMixedPolicyDeadlinesAndBorrowedSnapshots(t *testing.T) {
	p, opt := retirementPool(t)
	long := remote.ConnectionPolicy{Enabled: true, IdleTimeout: time.Hour, MaxIdleConnections: 4}
	short := remote.ConnectionPolicy{Enabled: true, IdleTimeout: 10 * time.Millisecond, MaxIdleConnections: 4}
	get := func(policy remote.ConnectionPolicy) *longConn {
		c, err := p.GetWithPolicy(context.Background(), "tcp", "127.0.0.1:8099", opt, policy)
		if err != nil {
			t.Fatal(err)
		}
		return c.(*longConn)
	}
	a, b := get(long), get(short)
	_ = p.Put(a)
	_ = p.Put(b)
	time.Sleep(15 * time.Millisecond)
	p.Evict()
	if a.Conn.(*retirementConn).closed.Load() != 0 || b.Conn.(*retirementConn).closed.Load() != 1 || p.globalIdle.Now() != 1 {
		t.Fatal("mixed deadlines must preserve old long lease and expire later short lease")
	}
	if c := get(long); c != a {
		t.Fatal("old snapshot could not reuse its compatible connection")
	} else {
		_ = p.Put(c)
	}
	c := get(short)
	if c == a || a.Conn.(*retirementConn).closed.Load() != 1 {
		t.Fatal("new policy borrowed a connection using old parameters")
	}
	_ = p.Discard(c)
}
