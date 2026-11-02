package remote

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

func TestUnaryHostDelayDisabledAllocations(t *testing.T) {
	c := &UnaryCall{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n := testing.AllocsPerRun(100, func() {
		if err := c.WaitResponseDelay(ctx, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}); n != 0 {
		t.Fatalf("disabled delay allocated %v", n)
	}
	if c.responseDelayApplied || c.ResponseDelayFacts.Outcome != 0 {
		t.Fatal("disabled policy created execution facts")
	}
}

func TestUnaryHostDelayCancelAndOnce(t *testing.T) {
	c := &UnaryCall{ResponseDelay: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	timer := time.AfterFunc(5*time.Millisecond, cancel)
	defer timer.Stop()
	first := time.Now()
	if err := c.WaitResponseDelay(ctx, first); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	facts := c.ResponseDelayFacts
	if facts.Outcome != UnaryHostWaitCanceled || facts.FirstResponseNS != first.UnixNano() || facts.DelayElapsedNS == 0 || facts.DelayElapsedNS >= uint64(time.Second) {
		t.Fatalf("partial wait facts %+v", facts)
	}
	if err := c.WaitResponseDelay(context.Background(), time.Now()); err != nil || c.ResponseDelayFacts != facts {
		t.Fatal("repeated response hook repeated the wait or changed first facts")
	}
}

func TestUnaryHostDelayConfiguredRPCBudgetWithoutContextDeadline(t *testing.T) {
	cfg := rpcinfo.NewRPCConfig()
	if err := rpcinfo.AsMutableRPCConfig(cfg).SetRPCTimeout(15 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ri := rpcinfo.NewRPCInfo(nil, nil, rpcinfo.NewInvocation("Mock", "Test"), cfg, nil)
	msg := NewMessage(nil, ri, Call, Client)
	defer RecycleMessage(msg)
	defer rpcinfo.PutRPCInfo(ri)
	c := &UnaryCall{ResponseDelay: time.Hour, Begin: time.Now(), Request: msg}
	if err := c.WaitResponseDelay(context.Background(), time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if c.ResponseDelayFacts.Outcome != UnaryHostWaitDeadline || c.ResponseDelayFacts.DelayElapsedNS == 0 || c.ResponseDelayFacts.DelayElapsedNS >= uint64(time.Second) {
		t.Fatalf("budget bypassed %+v", c.ResponseDelayFacts)
	}
}

func TestUnaryHostDelayCompletionAndOneway(t *testing.T) {
	c := &UnaryCall{ResponseDelay: 5 * time.Millisecond}
	if err := c.WaitResponseDelay(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if c.ResponseDelayFacts.Outcome != UnaryHostWaitCompleted || c.ResponseDelayFacts.DelayElapsedNS < uint64(c.ResponseDelay) {
		t.Fatalf("policy used as measured duration %+v", c.ResponseDelayFacts)
	}
	msg := NewMessage(nil, nil, Oneway, Client)
	defer RecycleMessage(msg)
	c = &UnaryCall{ResponseDelay: time.Hour, Request: msg}
	if err := c.WaitResponseDelay(context.Background(), time.Time{}); err != nil || c.ResponseDelayFacts.Outcome != 0 {
		t.Fatal("oneway waited for a response")
	}
}
