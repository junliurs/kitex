package client_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kt "github.com/cloudwego/kitex/internal/mocks/thrift"
	"github.com/cloudwego/kitex/internal/mocks/thrift/testservice"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
)

type lateBusinessEcho struct {
	kt.TestService
	entered chan struct{}
	release chan struct{}
	done    chan error
	exited  atomic.Bool
}

func (h *lateBusinessEcho) EchoPingPong(ctx context.Context, r *kt.Request) (*kt.Response, error) {
	if r.Message != "late" {
		return &kt.Response{Message: r.Message}, nil
	}
	ri := rpcinfo.GetRPCInfo(ctx)
	seq := ri.Invocation().SeqID()
	method := ri.Invocation().MethodName()
	close(h.entered)
	<-h.release
	var err error
	if r.Message != "late" || ri.Invocation().SeqID() != seq || ri.Invocation().MethodName() != method {
		err = errors.New("late handler observed recycled request or RPCInfo")
	}
	h.exited.Store(true)
	h.done <- err
	return &kt.Response{Message: "late"}, nil
}

func TestUnaryLifecycleLateBusiness(t *testing.T) {
	h := &lateBusinessEcho{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan error, 1)}
	var lateComplete atomic.Int32
	completed := make(chan struct{}, 1)
	hooks := &unaryHooks{complete: func(_ context.Context, c *remote.UnaryCall) error {
		if args, ok := c.Request.Data().(*kt.TestServiceEchoPingPongArgs); ok && args.Req.Message == "late" {
			if !h.exited.Load() {
				t.Error("Native completion ran before business exited")
			}
			if c.HandlerEnd.IsZero() || !c.HandlerEnd.After(c.WriteEnd) {
				t.Error("business/reply timestamps were merged", c.HandlerEnd, c.WriteEnd)
			}
			lateComplete.Add(1)
			completed <- struct{}{}
		}
		return nil
	}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ready, exit := make(chan struct{}), make(chan error)
	var calls atomic.Int32
	mw := func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp any) error {
			if calls.Add(1) != 1 {
				return next(ctx, req, resp)
			}
			workerCtx, release, err := remote.RetainUnaryExecution(ctx)
			if err != nil {
				return err
			}
			go func() {
				defer release()
				_ = next(workerCtx, req, resp)
				if rpcinfo.GetRPCInfo(workerCtx).Invocation().MethodName() != "EchoPingPong" {
					t.Error("middleware epilogue observed recycled RPCInfo")
				}
			}()
			<-h.entered
			return errors.New("outer timeout while business is running")
		}
	}
	s := testservice.NewServer(h, server.WithListener(ln), server.WithUnaryLifecycle(hooks), server.WithMiddleware(mw), server.WithExitWaitTime(time.Millisecond), server.WithExitSignal(func() <-chan error { close(ready); return exit }))
	stopped := make(chan error, 1)
	go func() { stopped <- s.Run() }()
	<-ready
	t.Cleanup(func() {
		select {
		case <-h.release:
		default:
			close(h.release)
		}
		close(exit)
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Error("server did not stop")
		}
	})
	c := unaryClient(t, ln.Addr().String(), &unaryHooks{})
	_, err = c.EchoPingPong(context.Background(), &kt.Request{Message: "late"})
	if err == nil || !strings.Contains(err.Error(), "outer timeout") {
		t.Fatal("missing early error reply", err)
	}
	if lateComplete.Load() != 0 {
		t.Error("completed early", lateComplete.Load())
	}
	for i := 0; i < 16; i++ {
		r, err := c.EchoPingPong(context.Background(), &kt.Request{Message: "other"})
		if err != nil || r.Message != "other" {
			t.Fatal("subsequent request", r, err)
		}
	}
	close(h.release)
	select {
	case err := <-h.done:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late handler not released")
	}
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("late completion missing")
	}
	if lateComplete.Load() != 1 {
		t.Fatal("late completion repeated", lateComplete.Load())
	}
}
