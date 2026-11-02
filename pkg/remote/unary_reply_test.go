package remote

import (
	"context"
	"errors"
	"testing"
)

type unaryReplyObserver struct {
	UnaryLifecycle
	replies, retirements int
	businessPending      bool
	replyError           error
}

func (o *unaryReplyObserver) ServerReplyCompleted(_ context.Context, c *UnaryCall) error {
	o.replies++
	o.businessPending = c.BusinessPending()
	o.replyError = c.ReplyError
	return nil
}

func (o *unaryReplyObserver) ServerComplete(_ context.Context, c *UnaryCall) error {
	o.retirements++
	if c.BusinessPending() {
		panic("retired live business")
	}
	return nil
}

func TestUnaryReplyBeforeBusiness(t *testing.T) {
	for _, writeErr := range []error{nil, errors.New("write failed")} {
		observer := new(unaryReplyObserver)
		c := NewUnaryCall(observer)
		ctx := WithUnaryCall(context.Background(), c)
		lease, err := BeginUnaryBusiness(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.Started, c.DeferComplete = true, true
		_ = c.CompleteServer(ctx, writeErr)
		_ = c.CompleteServer(ctx, errors.New("duplicate outer failure"))
		if observer.replies != 1 || observer.retirements != 0 || !observer.businessPending || observer.replyError != writeErr || c.ReplyError != writeErr {
			t.Fatalf("reply changed or retired early: %+v", observer)
		}
		lease.End(nil)
		c.SealBusiness()
		c.DeferComplete = false
		_ = c.CompleteServer(ctx, c.Err)
		_ = c.CompleteServer(ctx, c.Err)
		if observer.replies != 1 || observer.retirements != 1 {
			t.Fatalf("duplicate completion %+v", observer)
		}
		c.Recycle()
	}
}
