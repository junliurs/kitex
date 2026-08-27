/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package container

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/kitex/internal/test"
)

var _ Object = (*testObject)(nil)

type testObject struct {
	closeCallback func()
}

func (o *testObject) Close(exception error) error {
	if o.closeCallback != nil {
		o.closeCallback()
	}
	return nil
}

func TestObjectPool(t *testing.T) {
	op := NewObjectPool(time.Microsecond * 10)
	count := 10000
	key := "test"
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		o := new(testObject)
		o.closeCallback = func() {
			wg.Done()
		}
		wg.Add(1)
		op.Push(key, o)
	}
	wg.Wait()
	test.Assert(t, op.objects[key].Size() == 0)
	op.Close()

	// Keep the timeout well above the time needed to push 10k objects under
	// the race detector. This part verifies Close, not idle expiration.
	op = NewObjectPool(time.Hour)
	defer op.Close()
	var deleted int32
	for i := 0; i < count; i++ {
		o := new(testObject)
		o.closeCallback = func() {
			atomic.AddInt32(&deleted, 1)
		}
		op.Push(key, o)
	}
	test.Assert(t, atomic.LoadInt32(&deleted) == 0)
	test.Assert(t, op.objects[key].Size() == count)
	op.Close()
}

func TestObjectPool_cleaningLazyInit(t *testing.T) {
	// wait for all cleaning goroutines created by other tests finished
	waitForNoCleaningGoroutine(t, time.Second)
	op := NewObjectPool(10 * time.Microsecond)
	defer op.Close()
	if cleaningGoroutineExist() {
		t.Fatal("cleaning goroutine should not be started when ObjectPool.Push is not invoked")
	}
	op.Push("test", new(testObject))
	ticker := time.NewTicker(10 * time.Microsecond)
	timer := time.NewTimer(time.Millisecond)
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if cleaningGoroutineExist() {
				return
			}
		case <-timer.C:
			t.Fatal("cleaning goroutine should be started when ObjectPool.Push is invoked")
		}
	}
}

func cleaningGoroutineExist() bool {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "(*ObjectPool).cleaning") {
			return true
		}
	}
	return false
}

func waitForNoCleaningGoroutine(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !cleaningGoroutineExist() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("ObjectPool.cleaning goroutine did not stop")
}

func TestObjectPool_CloseStopsCleaningGoroutine(t *testing.T) {
	waitForNoCleaningGoroutine(t, time.Second)

	op := NewObjectPool(time.Hour)
	op.Push("test", new(testObject))

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cleaningGoroutineExist() {
			break
		}
		time.Sleep(10 * time.Microsecond)
	}
	if !cleaningGoroutineExist() {
		t.Fatal("cleaning goroutine should have started after Push")
	}

	op.Close()

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if !cleaningGoroutineExist() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("ObjectPool.cleaning goroutine still running after Close")
}

func TestObjectPool_CloseDrainsPooledObjects(t *testing.T) {
	op := NewObjectPool(time.Hour)
	var closed int32
	count := 5
	for i := 0; i < count; i++ {
		o := new(testObject)
		o.closeCallback = func() {
			atomic.AddInt32(&closed, 1)
		}
		op.Push("test", o)
	}

	op.Close()

	if atomic.LoadInt32(&closed) != int32(count) {
		t.Fatalf("expected %d objects closed, got %d", count, atomic.LoadInt32(&closed))
	}
}

func TestObjectPool_CloseHandlesNilObject(t *testing.T) {
	op := NewObjectPool(time.Hour)
	op.Push("test", nil)
	op.Close()
}

func TestObjectPool_PushAfterCloseClosesObject(t *testing.T) {
	op := NewObjectPool(time.Hour)
	op.Close()

	var closed int32
	o := new(testObject)
	o.closeCallback = func() {
		atomic.AddInt32(&closed, 1)
	}
	op.Push("test", o)

	if atomic.LoadInt32(&closed) != 1 {
		t.Fatal("object pushed after Close should be closed immediately")
	}
}

func TestObjectPool_PushNilAfterClose(t *testing.T) {
	op := NewObjectPool(time.Hour)
	op.Close()
	op.Push("test", nil)
}
