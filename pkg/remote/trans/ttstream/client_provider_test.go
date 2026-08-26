/*
 * Copyright 2025 CloudWeGo Authors
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

package ttstream

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
	"github.com/cloudwego/kitex/pkg/remote/trans/ttstream/ktx"
)

type cancelTestWriter struct {
	closed int32
}

func (w *cancelTestWriter) WriteFrame(*Frame) error {
	return nil
}

func (w *cancelTestWriter) CloseStream(int32) error {
	atomic.AddInt32(&w.closed, 1)
	return nil
}

func Test_registerStreamCancelCallback(t *testing.T) {
	ctx, cancel := ktx.WithCancel(context.Background())
	s := &stream{peerEOF: 1}
	registerStreamCancelCallback(ctx, s)
	cancel()
	test.Assert(t, atomic.LoadInt32(&s.selfEOF) == 1)
}

func Test_registerStreamCancelCallbackClosesStream(t *testing.T) {
	ctx, cancel := ktx.WithCancel(context.Background())
	w := new(cancelTestWriter)
	s := newStream(ctx, w, streamFrame{sid: 1})
	registerStreamCancelCallback(ctx, s)

	cancel()

	test.Assert(t, atomic.LoadInt32(&s.peerEOF) == 1)
	test.Assert(t, atomic.LoadInt32(&s.selfEOF) == 1)
	test.Assert(t, atomic.LoadInt32(&s.eofFlag) == 2)
	test.Assert(t, atomic.LoadInt32(&w.closed) == 1)
}
