package client_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/client"
	kt "github.com/cloudwego/kitex/internal/mocks/thrift"
	"github.com/cloudwego/kitex/pkg/connpool"
	"github.com/cloudwego/kitex/pkg/discovery"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo/remoteinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/cloudwego/kitex/transport"
)

type onewayHeaderHooks struct{ *unaryHooks }

func (*onewayHeaderHooks) HeaderBeforePayload() bool { return true }
func (h *onewayHeaderHooks) ServerHeaderReady(ctx context.Context, c *remote.UnaryCall) error {
	c.State = true
	return h.unaryHooks.ServerBegin(ctx, c)
}

func (h *onewayHeaderHooks) ServerBegin(ctx context.Context, c *remote.UnaryCall) error {
	if c.State == true {
		return nil
	}
	return h.unaryHooks.ServerBegin(ctx, c)
}

func TestUnaryLifecycleOneway(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "success"
		if reject {
			name = "early_reject"
		}
		t.Run(name, func(t *testing.T) {
			var started, finish, response, complete atomic.Int32
			entered, completed := make(chan struct{}), make(chan struct{})
			gate := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(gate) })
			info := &serviceinfo.ServiceInfo{ServiceName: "MeshlibOneway", PayloadCodec: serviceinfo.Thrift, Methods: map[string]serviceinfo.MethodInfo{}}
			info.Methods["Notify"] = serviceinfo.NewMethodInfo(func(_ context.Context, _, args, result any) error {
				started.Add(1)
				close(entered)
				<-gate
				return nil
			}, func() any { return new(kt.TestServiceEchoPingPongArgs) }, func() any { return nil }, true)
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			sh := &onewayHeaderHooks{&unaryHooks{begin: func(_ context.Context, c *remote.UnaryCall) error {
				if !c.OneWay() || c.RequestWire.MessageType != uint32(remote.Oneway) {
					return errors.New("oneway missing before payload")
				}
				if reject {
					return errors.New("deny")
				}
				return nil
			}, response: func(context.Context, *remote.UnaryCall) error {
				response.Add(1)
				return errors.New("oneway response hook")
			}, complete: func(_ context.Context, c *remote.UnaryCall) error {
				complete.Add(1)
				if !c.WriteStart.IsZero() || !c.WriteEnd.IsZero() {
					t.Error("oneway performed a reply write")
				}
				close(completed)
				return nil
			}}}
			ready, exit := make(chan struct{}), make(chan error)
			svr := server.NewServer(server.WithListener(ln), server.WithUnaryLifecycle(sh), server.WithExitWaitTime(time.Millisecond), server.WithExitSignal(func() <-chan error { close(ready); return exit }))
			if err = svr.RegisterService(info, &struct{}{}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- svr.Run() }()
			t.Cleanup(func() {
				release.Do(func() { close(gate) })
				close(exit)
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(time.Second):
					t.Error("server stop")
				}
			})
			select {
			case <-ready:
			case <-time.After(time.Second):
				t.Fatal("server start")
			}
			ch := &unaryHooks{prepare: func(_ context.Context, c *remote.UnaryCall) error {
				if !c.OneWay() || c.RequestWire.MessageType != uint32(remote.Oneway) {
					return errors.New("client oneway missing")
				}
				remoteinfo.AsRemoteInfo(c.Request.RPCInfo().To()).SetInstance(discovery.NewInstance("tcp", ln.Addr().String(), 1, nil))
				return nil
			}, finish: func(_ context.Context, c *remote.UnaryCall) error {
				finish.Add(1)
				if c.Response != nil || !c.ReadEnd.IsZero() {
					return errors.New("oneway attempted response read")
				}
				return nil
			}}
			cli, err := client.NewClient(info, client.WithDestService("meshlib.oneway"), client.WithUnaryLifecycle(ch), client.WithTransportProtocol(transport.TTHeaderFramed), client.WithRPCTimeout(time.Second), client.WithLongConnection(connpool.IdleConfig{MaxIdlePerAddress: 1, MaxIdleGlobal: 1, MaxIdleTimeout: time.Minute}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if c, ok := cli.(interface{ Close() error }); ok {
					_ = c.Close()
				}
			})
			sent := make(chan error, 1)
			go func() {
				sent <- cli.Call(context.Background(), "Notify", &kt.TestServiceEchoPingPongArgs{Req: &kt.Request{Message: "oneway"}}, nil)
			}()
			select {
			case err := <-sent:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("oneway waited for response")
			}
			if !reject {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("handler not entered")
				}
				select {
				case <-completed:
					t.Fatal("completed before handler exit")
				default:
				}
			}
			release.Do(func() { close(gate) })
			select {
			case <-completed:
			case <-time.After(time.Second):
				t.Fatal("server complete missing")
			}
			want := int32(1)
			if reject {
				want = 0
			}
			if started.Load() != want || finish.Load() != 1 || response.Load() != 0 || complete.Load() != 1 {
				t.Fatalf("started=%d finish=%d response=%d complete=%d", started.Load(), finish.Load(), response.Load(), complete.Load())
			}
		})
	}
}
