package remote

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestUnaryExecutionLateEntry(t *testing.T) {
	c := NewUnaryCall(nil)
	ctx := WithUnaryCall(context.Background(), c)
	if c.SealBusiness() {
		t.Fatal("no handler should be active")
	}
	if _, err := BeginUnaryBusiness(ctx); !errors.Is(err, ErrUnaryBusinessRetired) {
		t.Fatal("late business started", err)
	}
	c.Recycle()
	if UnaryCallFromContext(ctx) != nil {
		t.Fatal("retired context still exposes pooled call")
	}
	next := NewUnaryCall(nil)
	defer next.Recycle()
	if _, err := BeginUnaryBusiness(ctx); !errors.Is(err, ErrUnaryBusinessRetired) {
		t.Fatal("old context attached after reuse", err)
	}
}

func TestUnaryExecutionRetirementRace(t *testing.T) {
	for i := 0; i < 10000; i++ {
		c := NewUnaryCall(nil)
		ctx := WithUnaryCall(context.Background(), c)
		lease, err := BeginUnaryBusiness(ctx)
		if err != nil {
			t.Fatal(err)
		}
		completed := make(chan struct{})
		worker := make(chan struct{})
		var cleanups atomic.Int32
		if i%2 == 0 {
			lease.End(nil)
			close(worker)
		} else {
			go func() { lease.End(nil); close(worker) }()
		}
		c.SealBusiness()
		c.RetireAfterBusiness(func() {
			c.SnapshotBusiness()
			if c.HandlerStart.IsZero() || c.HandlerEnd.IsZero() {
				t.Error("missing actual business times")
			}
			cleanups.Add(1)
			c.Recycle()
			close(completed)
		})
		<-completed
		<-worker
		if cleanups.Load() != 1 || UnaryCallFromContext(ctx) != nil {
			t.Fatal("call reused or cleanup duplicated", i)
		}
		next := NewUnaryCall(nil)
		nextCtx := WithUnaryCall(context.Background(), next)
		nextLease, err := BeginUnaryBusiness(nextCtx)
		if err != nil {
			t.Fatal(err)
		}
		lease.End(errors.New("duplicate old completion"))
		if !next.BusinessPending() {
			t.Fatal("stale lease ended a new business", i)
		}
		nextLease.End(nil)
		next.SealBusiness()
		next.Recycle()
	}
}

func TestUnaryExecutionRetainedTask(t *testing.T) {
	c := NewUnaryCall(nil)
	ctx := WithUnaryCall(context.Background(), c)
	worker, release, err := RetainUnaryExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !c.SealBusiness() {
		t.Fatal("registered worker was not retained")
	}
	child, releaseChild, err := RetainUnaryExecution(worker)
	if err != nil {
		t.Fatal("registered task lost its child scope", err)
	}
	lease, err := BeginUnaryBusiness(child)
	if err != nil {
		t.Fatal("registered business entry was rejected", err)
	}
	lease.End(nil)
	var completed atomic.Int32
	c.RetireAfterBusiness(func() { completed.Add(1); c.Recycle() })
	release()
	release()
	if completed.Load() != 0 {
		t.Fatal("retired before middleware epilogue left")
	}
	releaseChild()
	releaseChild()
	if completed.Load() != 1 || UnaryCallFromContext(ctx) != nil {
		t.Fatal("task was not retired exactly once")
	}
	if _, err := BeginUnaryBusiness(worker); !errors.Is(err, ErrUnaryBusinessRetired) {
		t.Fatal("old worker attached to another request", err)
	}
}

func TestUnaryExecutionCanceledEntry(t *testing.T) {
	c := NewUnaryCall(nil)
	base, cancel := context.WithCancel(context.Background())
	ctx := WithUnaryCall(base, c)
	worker, release, err := RetainUnaryExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = BeginUnaryBusiness(worker); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled work entered business", err)
	}
	c.SealBusiness()
	var completed atomic.Int32
	c.RetireAfterBusiness(func() { completed.Add(1); c.Recycle() })
	release()
	if completed.Load() != 1 {
		t.Fatal("canceled worker leaked its request")
	}
}
