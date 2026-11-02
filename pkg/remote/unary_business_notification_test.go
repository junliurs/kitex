package remote

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestUnaryBusinessNotificationBeforeEpilogue(t *testing.T) {
	c := NewUnaryCall(nil)
	ctx, release, err := RetainUnaryExecution(WithUnaryCall(context.Background(), c))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := BeginUnaryBusiness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.SealBusiness()
	var notified, retired int
	c.RetireAfterBusinessWithNotification(func() {
		c.SnapshotBusiness()
		notified++
		if c.HandlerEnd.IsZero() || !c.BusinessPending() {
			t.Error("lost business facts or epilogue hold")
		}
	}, func() { retired++; c.Recycle() })
	lease.End(nil)
	if notified != 1 || retired != 0 {
		t.Fatal("business notification waited for epilogue", notified, retired)
	}
	release()
	if notified != 1 || retired != 1 {
		t.Fatal("retirement", notified, retired)
	}
}

func TestUnaryBusinessNotificationRetirementRace(t *testing.T) {
	for i := 0; i < 3000; i++ {
		c := NewUnaryCall(nil)
		ctx, release, err := RetainUnaryExecution(WithUnaryCall(context.Background(), c))
		if err != nil {
			t.Fatal(err)
		}
		var lease UnaryBusinessLease
		if i%3 != 0 {
			lease, err = BeginUnaryBusiness(ctx)
			if err != nil {
				t.Fatal(err)
			}
		}
		c.SealBusiness()
		var notified, retired atomic.Int32
		finished, worker := make(chan struct{}), make(chan struct{})
		go func() { lease.End(nil); release(); close(worker) }()
		c.RetireAfterBusinessWithNotification(func() {
			c.SnapshotBusiness()
			notified.Add(1)
		}, func() {
			if notified.Load() != 1 {
				t.Error("retirement overtook notification")
			}
			retired.Add(1)
			c.Recycle()
			close(finished)
		})
		<-finished
		<-worker
		if notified.Load() != 1 || retired.Load() != 1 {
			t.Fatal("duplicate callback", i)
		}
	}
}

func TestUnaryBusinessNotificationBlocked(t *testing.T) {
	c := NewUnaryCall(nil)
	ctx, release, err := RetainUnaryExecution(WithUnaryCall(context.Background(), c))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := BeginUnaryBusiness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c.SealBusiness()
	entered, unblock, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c.RetireAfterBusinessWithNotification(func() { close(entered); <-unblock }, func() { c.Recycle(); close(finished) })
	worker := make(chan struct{})
	go func() { lease.End(nil); close(worker) }()
	<-entered
	release()
	select {
	case <-finished:
		t.Fatal("retired during business publication")
	default:
	}
	close(unblock)
	<-finished
	<-worker
}
