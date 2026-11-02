package remote

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestUnaryBusinessObserverBlocksRetirement(t *testing.T) {
	c := NewUnaryCall(nil)
	ctx := WithUnaryCall(context.Background(), c)
	entered, unblock, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	problem := errors.New("Native fact rejected")
	if err := c.ObserveBusinessCompletion(func(start, end int64, businessError error) error {
		if start <= 0 || end < start || businessError != nil {
			t.Error("wrong actual handler facts")
		}
		close(entered)
		<-unblock
		return problem
	}); err != nil {
		t.Fatal(err)
	}
	lease, err := BeginUnaryBusiness(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.ObserveBusinessCompletion(func(int64, int64, error) error { return nil }) == nil {
		t.Fatal("observer installed after business began")
	}
	c.SealBusiness()
	c.RetireAfterBusiness(func() {
		c.SnapshotBusiness()
		if c.HandlerError != nil || !errors.Is(c.BusinessNotificationError, problem) {
			t.Error("observer error changed business outcome")
		}
		c.Recycle()
		close(finished)
	})
	worker := make(chan struct{})
	go func() { lease.End(nil); close(worker) }()
	<-entered
	select {
	case <-finished:
		t.Fatal("retired before fact-only observer returned")
	default:
	}
	close(unblock)
	<-finished
	<-worker
}

func TestUnaryBusinessObserverPanicPreservesHandlerError(t *testing.T) {
	c := NewUnaryCall(nil)
	defer c.Recycle()
	if err := c.ObserveBusinessCompletion(func(int64, int64, error) error { panic("observer failure") }); err != nil {
		t.Fatal(err)
	}
	lease, err := BeginUnaryBusiness(WithUnaryCall(context.Background(), c))
	if err != nil {
		t.Fatal(err)
	}
	problem := errors.New("actual business failure")
	lease.End(problem)
	lease.End(nil)
	c.SnapshotBusiness()
	if !errors.Is(c.HandlerError, problem) || c.BusinessNotificationError == nil || !strings.Contains(c.BusinessNotificationError.Error(), "observer failure") {
		t.Fatal("lost independent errors", c.HandlerError, c.BusinessNotificationError)
	}
}
