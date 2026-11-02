package remote

import (
	"context"
	"errors"
	"time"
)

// UnaryHostIOFacts records host execution, independently of the requested
// delay. FirstResponseNS is the client's first successful byte peek; ingress
// response delays leave it zero. Elapsed uses Go's monotonic clock.
type UnaryHostIOFacts struct {
	FirstResponseNS, DelayStartNS, DelayEndNS int64
	DelayElapsedNS                            uint64
	Outcome                                   uint32
}

const (
	UnaryHostWaitCompleted uint32 = 1
	UnaryHostWaitCanceled  uint32 = 2
	UnaryHostWaitDeadline  uint32 = 3
)

// WaitResponseDelay is invoked at the actual response I/O point, once per
// attempt. Configured RPC budgets still apply when ctx has no deadline.
func (c *UnaryCall) WaitResponseDelay(ctx context.Context, firstResponse time.Time) (err error) {
	if c == nil || c.ResponseDelay <= 0 || c.responseDelayApplied || c.OneWay() {
		return nil
	}
	c.responseDelayApplied = true
	start := time.Now()
	facts := &c.ResponseDelayFacts
	facts.DelayStartNS = start.UnixNano()
	if !firstResponse.IsZero() {
		facts.FirstResponseNS = firstResponse.UnixNano()
	}
	defer func() {
		end := time.Now()
		facts.DelayEndNS, facts.DelayElapsedNS = end.UnixNano(), uint64(end.Sub(start))
		facts.Outcome = UnaryHostWaitCompleted
		if errors.Is(err, context.DeadlineExceeded) {
			facts.Outcome = UnaryHostWaitDeadline
		} else if err != nil {
			facts.Outcome = UnaryHostWaitCanceled
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if c.Request != nil && c.Request.RPCInfo() != nil && !c.Begin.IsZero() {
		if timeout := c.Request.RPCInfo().Config().RPCTimeout(); timeout > 0 {
			deadline := c.Begin.Add(timeout)
			if parent, ok := ctx.Deadline(); !ok || deadline.Before(parent) {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, deadline)
				defer cancel()
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(c.ResponseDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
