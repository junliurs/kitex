package remote

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestUnaryOnewayCancellationScope(t *testing.T) {
	deadlineRoot, stopTimer := context.WithTimeout(context.Background(), time.Minute)
	defer stopTimer()
	parent, stop := context.WithCancelCause(deadlineRoot)
	defer stop(nil)
	connection := WithUnaryConnectionCancellation(parent)
	call := NewUnaryCall(nil)
	defer call.Recycle()
	call.Request = NewMessage(nil, nil, Oneway, Server)
	defer RecycleMessage(call.Request)
	key := struct{ label string }{"value"}
	request := ContextForUnaryRequest(context.WithValue(connection, key, "kept"), call)
	deadline, ok := request.Deadline()
	want, _ := parent.Deadline()
	if !ok || deadline != want || request.Value(key) != "kept" {
		t.Fatal("lost request values or server lifetime")
	}
	business, cancel := context.WithCancel(request)
	defer cancel()
	CancelUnaryConnection(connection)
	if request.Err() != nil || context.Cause(request) != nil || business.Err() != nil {
		t.Fatal("peer close canceled one-way work")
	}
	cause := errors.New("server stopping")
	stop(cause)
	select {
	case <-business.Done():
	case <-time.After(time.Second):
		t.Fatal("server lifetime did not cancel business")
	}
	if request.Err() != context.Canceled || context.Cause(request) != cause || context.Cause(business) != cause {
		t.Fatal("server cancellation cause lost", request.Err(), context.Cause(request), context.Cause(business))
	}
}
