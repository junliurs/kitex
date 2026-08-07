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

package ttstream

import (
	"bytes"
	"context"
	"testing"

	"github.com/bytedance/gopkg/lang/mcache"
	"github.com/cloudwego/gopkg/bufiox"
	gopkgthrift "github.com/cloudwego/gopkg/protocol/thrift"

	"github.com/cloudwego/kitex/internal/test"
)

func TestFrameCodec(t *testing.T) {
	var buf bytes.Buffer
	writer := bufiox.NewDefaultWriter(&buf)
	reader := bufiox.NewDefaultReader(&buf)
	wframe := newFrame(streamFrame{
		sid:    0,
		method: "method",
		header: map[string]string{"key": "value"},
	}, headerFrameType, []byte("hello world"))

	for i := 0; i < 10; i++ {
		wframe.sid = int32(i)
		err := EncodeFrame(context.Background(), writer, wframe)
		test.Assert(t, err == nil, err)
	}
	err := writer.Flush()
	test.Assert(t, err == nil, err)

	for i := 0; i < 10; i++ {
		rframe, err := DecodeFrame(context.Background(), reader)
		test.Assert(t, err == nil, err)
		test.DeepEqual(t, string(wframe.payload), string(rframe.payload))
		test.DeepEqual(t, wframe.header, rframe.header)
	}

	for i := 0; i < 10; i++ {
		wframe.sid = int32(i)
		err = encodeFrameAndFlush(context.Background(), writer, wframe)
		test.Assert(t, err == nil, err)
	}
	err = writer.Flush()
	test.Assert(t, err == nil, err)

	for i := 0; i < 10; i++ {
		rframe, err := DecodeFrame(context.Background(), reader)
		test.Assert(t, err == nil, err)
		test.DeepEqual(t, string(wframe.payload), string(rframe.payload))
		test.DeepEqual(t, wframe.header, rframe.header)
	}
}

func TestFrameWithoutPayloadCodec(t *testing.T) {
	rmsg := new(testRequest)
	rmsg.A = 1
	payload, err := EncodePayload(context.Background(), rmsg)
	test.Assert(t, err == nil, err)

	wmsg := new(testRequest)
	err = DecodePayload(context.Background(), payload, wmsg)
	test.Assert(t, err == nil, err)
	test.DeepEqual(t, wmsg, rmsg)
}

func TestEncodeStreamPayload(t *testing.T) {
	messageAtSize := func(size int) *testRequest {
		message := &testRequest{A: 1}
		message.B = string(make([]byte, size-message.BLength()))
		test.Assert(t, message.BLength() == size, message.BLength())
		return message
	}
	for _, testCase := range []struct {
		name       string
		message    *testRequest
		wantPooled bool
	}{
		{
			name:       "small",
			message:    &testRequest{A: 1, B: "hello world"},
			wantPooled: true,
		},
		{
			name:       "pool boundary",
			message:    messageAtSize(maxPooledStreamPayloadSize),
			wantPooled: true,
		},
		{
			name:       "above pool boundary",
			message:    messageAtSize(maxPooledStreamPayloadSize + 1),
			wantPooled: false,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			want := gopkgthrift.FastMarshal(testCase.message)
			payload, pooled, err := encodeStreamPayload(
				context.Background(),
				testCase.message,
			)
			test.Assert(t, err == nil, err)
			test.Assert(t, pooled == testCase.wantPooled, pooled)
			test.Assert(t, bytes.Equal(payload, want))
			if pooled {
				mcache.Free(payload)
			}
		})
	}
}

func BenchmarkEncodeStreamPayload(b *testing.B) {
	message := &testRequest{A: 1, B: string(make([]byte, 1024))}

	b.Run("FastMarshal", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = gopkgthrift.FastMarshal(message)
		}
	})
	b.Run("MCache", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			payload, pooled, err := encodeStreamPayload(
				context.Background(),
				message,
			)
			if err != nil || !pooled {
				b.Fatalf("pooled=%t, err=%v", pooled, err)
			}
			mcache.Free(payload)
		}
	})
}

func TestPayloadCodec(t *testing.T) {
	rmsg := new(testRequest)
	rmsg.A = 1
	rmsg.B = "hello world"
	payload, err := EncodePayload(context.Background(), rmsg)
	test.Assert(t, err == nil, err)

	wmsg := new(testRequest)
	err = DecodePayload(context.Background(), payload, wmsg)
	test.Assert(t, err == nil, err)
	test.DeepEqual(t, wmsg, rmsg)
}
