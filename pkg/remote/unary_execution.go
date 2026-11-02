package remote

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	executionStarted uint64 = 1 << iota
	executionDone
	executionSealed
	executionRetiring
	executionRetired
	executionCompleting
	executionCallbackReady
	executionNotifyReady
	executionNotifying
	executionNotified
	executionObserverInstalling
	executionObserverReady
	executionScope     = uint64(1) << 12
	executionScopeMask = uint64(65535) << 12
	executionEpoch     = uint64(1) << 28
	executionFlags     = executionEpoch - 1
)

var ErrUnaryBusinessRetired = errors.New("unary business entry is closed or already used")

// One stable execution record per pooled call. Old contexts retain its epoch,
// so late middleware cannot attach to a different request reusing the call.
type unaryExecution struct {
	state          atomic.Uint64
	start, end     atomic.Int64
	businessErr    error  // Published by executionDone, read only after an acquire.
	cleanup        func() // Published by executionCallbackReady.
	notifyBusiness func() // Published by executionNotifyReady.
	observer       UnaryBusinessObserver
	observerErr    error // Published by executionDone.
}

// Keep this path available on the SDK's Go 1.21 minimum.
func (e *unaryExecution) addFlags(flags uint64) {
	for {
		state := e.state.Load()
		if e.state.CompareAndSwap(state, state|flags) {
			return
		}
	}
}

func (e *unaryExecution) begin() {
	next := (e.state.Load() &^ executionFlags) + executionEpoch
	e.start.Store(0)
	e.end.Store(0)
	e.businessErr = nil
	e.cleanup = nil
	e.notifyBusiness = nil
	e.observer = nil
	e.observerErr = nil
	e.state.Store(next)
}

type unaryCallContext struct {
	context.Context
	call      *UnaryCall
	execution *unaryExecution
	epoch     uint64
}

func (c *unaryCallContext) Value(key any) any {
	if _, ok := key.(unaryCallKey); ok {
		return c
	}
	return c.Context.Value(key)
}

func (c *unaryCallContext) live() bool {
	state := c.execution.state.Load()
	return state&^executionFlags == c.epoch && state&executionRetired == 0
}

type UnaryBusinessLease struct {
	call      *UnaryCall
	execution *unaryExecution
	epoch     uint64
}

func BeginUnaryBusiness(ctx context.Context) (UnaryBusinessLease, error) {
	reference, _ := ctx.Value(unaryCallKey{}).(*unaryCallContext)
	if reference == nil {
		return UnaryBusinessLease{}, nil
	}
	e := reference.execution
	for {
		state := e.state.Load()
		if state&^executionFlags != reference.epoch || state&(executionStarted|executionRetiring|executionRetired|executionObserverInstalling) != 0 {
			return UnaryBusinessLease{}, ErrUnaryBusinessRetired
		}
		if state&executionSealed != 0 {
			task, _ := ctx.Value(unaryTaskKey{}).(*unaryTaskLease)
			if task == nil || task.epoch != reference.epoch || task.released.Load() {
				return UnaryBusinessLease{}, ErrUnaryBusinessRetired
			}
		}
		if err := ctx.Err(); err != nil {
			return UnaryBusinessLease{}, err
		}
		if e.state.CompareAndSwap(state, state|executionStarted) {
			e.start.Store(time.Now().UnixNano())
			return UnaryBusinessLease{reference.call, e, reference.epoch}, nil
		}
	}
}

func (l UnaryBusinessLease) End(err error) {
	if l.execution == nil {
		return
	}
	e := l.execution
	for {
		state := e.state.Load()
		if state&^executionFlags != l.epoch || state&(executionCompleting|executionDone|executionRetired) != 0 {
			return
		}
		if e.state.CompareAndSwap(state, state|executionCompleting) {
			break
		}
	}
	e.businessErr = err
	e.end.Store(time.Now().UnixNano())
	if e.observer != nil {
		e.observerErr = e.observeBusiness(e.start.Load(), e.end.Load(), err)
	}
	e.addFlags(executionDone)
	e.tryCompletion(l.epoch)
}

// Business fact publication and resource retirement are separate. Only this
// notifier may write the host/Native call after transport relinquishes it.
func (e *unaryExecution) tryCompletion(epoch uint64) {
	for {
		state := e.state.Load()
		if state&^executionFlags != epoch || state&executionRetired != 0 {
			return
		}
		if state&executionNotifyReady == 0 || state&executionNotified != 0 {
			break
		}
		if state&executionNotifying != 0 {
			return
		}
		if state&executionStarted != 0 {
			if state&executionDone == 0 {
				return
			}
		} else if state&executionSealed == 0 || state&executionScopeMask != 0 {
			return
		}
		if !e.state.CompareAndSwap(state, state|executionNotifying) {
			continue
		}
		notify := e.notifyBusiness
		e.notifyBusiness = nil
		notify()
		e.addFlags(executionNotified)
		break
	}
	e.tryCleanup(epoch)
}

func (e *unaryExecution) tryCleanup(epoch uint64) {
	for {
		state := e.state.Load()
		if state&^executionFlags != epoch || state&executionRetired != 0 || state&executionCallbackReady == 0 || state&executionRetiring != 0 || state&executionScopeMask != 0 || (state&executionNotifyReady != 0 && state&executionNotified == 0) || (state&executionStarted != 0 && state&executionDone == 0) {
			return
		}
		if e.state.CompareAndSwap(state, state|executionRetiring) {
			cleanup := e.cleanup
			e.cleanup = nil
			cleanup()
			return
		}
	}
}

func (c *UnaryCall) BusinessPending() bool {
	if c == nil || c.execution == nil {
		return false
	}
	state := c.execution.state.Load()
	return (state&executionStarted != 0 && state&executionDone == 0) || state&executionScopeMask != 0
}

func (c *UnaryCall) SnapshotBusiness() {
	if c.execution == nil {
		return
	}
	if n := c.execution.start.Load(); n != 0 {
		c.HandlerStart = time.Unix(0, n)
	}
	if c.execution.state.Load()&executionDone != 0 {
		if n := c.execution.end.Load(); n != 0 {
			c.HandlerEnd = time.Unix(0, n)
		}
		c.HandlerError = c.execution.businessErr
		c.BusinessNotificationError = c.execution.observerErr
	}
}

// Seal before deciding whether to recycle. An unstarted background next(ctx)
// must not begin after the transport has returned and released its arguments.
func (c *UnaryCall) SealBusiness() bool {
	if c.execution == nil {
		return false
	}
	c.execution.addFlags(executionSealed)
	return c.BusinessPending()
}

func (c *UnaryCall) RetireAfterBusiness(cleanup func()) {
	c.RetireAfterBusinessWithNotification(nil, cleanup)
}

// Install only after the transport has finished consuming all Native outputs.
// notify runs once at actual handler exit; cleanup also waits for task scopes.
func (c *UnaryCall) RetireAfterBusinessWithNotification(notify, cleanup func()) {
	e := c.execution
	epoch := e.state.Load() &^ executionFlags
	e.cleanup, e.notifyBusiness = cleanup, notify
	flags := executionCallbackReady
	if notify != nil {
		flags |= executionNotifyReady
	}
	e.addFlags(flags)
	e.tryCompletion(epoch)
}

// RetainUnaryExecution must surround a framework-owned asynchronous next(ctx)
// call, including its middleware epilogues. Ordinary synchronous calls need no
// extra lease. Registration happens before launching the worker.
type (
	unaryTaskKey   struct{}
	unaryTaskLease struct {
		execution *unaryExecution
		epoch     uint64
		released  atomic.Bool
	}
)

func RetainUnaryExecution(ctx context.Context) (context.Context, func(), error) {
	reference, _ := ctx.Value(unaryCallKey{}).(*unaryCallContext)
	if reference == nil {
		return ctx, nil, ErrUnaryBusinessRetired
	}
	e := reference.execution
	for {
		state := e.state.Load()
		if state&^executionFlags != reference.epoch || state&(executionRetiring|executionRetired) != 0 || state&executionScopeMask == executionScopeMask {
			return ctx, nil, ErrUnaryBusinessRetired
		}
		if state&executionSealed != 0 {
			parent, _ := ctx.Value(unaryTaskKey{}).(*unaryTaskLease)
			if parent == nil || parent.epoch != reference.epoch || parent.released.Load() {
				return ctx, nil, ErrUnaryBusinessRetired
			}
		}
		if e.state.CompareAndSwap(state, state+executionScope) {
			break
		}
	}
	task := &unaryTaskLease{execution: e, epoch: reference.epoch}
	return context.WithValue(ctx, unaryTaskKey{}, task), task.release, nil
}

func (task *unaryTaskLease) release() {
	if !task.released.CompareAndSwap(false, true) {
		return
	}
	task.execution.state.Add(^uint64(executionScope - 1))
	task.execution.tryCompletion(task.epoch)
}

// UnaryBusinessObserver may overlap transport I/O. It must only publish copied
// facts through a concurrency-safe host adapter, never mutate the UnaryCall.
type UnaryBusinessObserver func(startNS, endNS int64, businessError error) error

// Install after admission and before launching business execution.
func (c *UnaryCall) ObserveBusinessCompletion(observer UnaryBusinessObserver) error {
	if c == nil || c.execution == nil || observer == nil {
		return ErrUnaryBusinessRetired
	}
	e := c.execution
	for {
		state := e.state.Load()
		if state&(executionStarted|executionSealed|executionRetired|executionObserverInstalling|executionObserverReady) != 0 {
			return ErrUnaryBusinessRetired
		}
		if !e.state.CompareAndSwap(state, state|executionObserverInstalling) {
			continue
		}
		e.observer = observer
		for {
			state = e.state.Load()
			if e.state.CompareAndSwap(state, (state&^executionObserverInstalling)|executionObserverReady) {
				return nil
			}
		}
	}
}

func (e *unaryExecution) observeBusiness(start, end int64, businessError error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("business completion observer panicked: %v", value)
		}
	}()
	return e.observer(start, end, businessError)
}
