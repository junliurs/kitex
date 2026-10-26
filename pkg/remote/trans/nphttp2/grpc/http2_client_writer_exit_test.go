/*
 * Copyright 2026 CloudWeGo Authors
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

package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwego/netpoll"
	"golang.org/x/net/http2"
)

// writerExitConn holds a failed write until the test has arranged the reader
// and an existing stream. Reads remain open until the transport closes them.
type writerExitConn struct {
	net.Conn
	failWrites   atomic.Bool
	writeStarted chan struct{}
	releaseWrite chan struct{}
	writeErr     error
}

func (c *writerExitConn) Write(p []byte) (int, error) {
	if c.failWrites.Load() {
		close(c.writeStarted)
		<-c.releaseWrite
		return 0, c.writeErr
	}
	return c.Conn.Write(p)
}

func TestClientTransportWriterExitClosesTransport(t *testing.T) {
	for _, tc := range []struct {
		name           string
		writeErr       error
		throttleReader bool
		closeReader    bool
	}{
		{
			name:     "net_error",
			writeErr: netpoll.Exception(netpoll.ErrConnClosed, "when write"),
		},
		{
			name:     "non_net_error",
			writeErr: io.ErrClosedPipe,
		},
		{
			name:           "net_error_with_throttled_reader",
			writeErr:       netpoll.Exception(netpoll.ErrConnClosed, "when write"),
			throttleReader: true,
		},
		{
			name:           "non_net_error_with_throttled_reader",
			writeErr:       io.ErrClosedPipe,
			throttleReader: true,
		},
		{
			name:        "reader_writer_close_race",
			writeErr:    netpoll.Exception(netpoll.ErrConnClosed, "when write"),
			closeReader: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, server := net.Pipe()
			conn := &writerExitConn{
				Conn:         raw,
				writeStarted: make(chan struct{}),
				releaseWrite: make(chan struct{}),
				writeErr:     tc.writeErr,
			}
			var releaseOnce sync.Once
			releaseWrite := func() { releaseOnce.Do(func() { close(conn.releaseWrite) }) }
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ackRead := make(chan struct{})
			sendPing := make(chan struct{})
			stopServer := make(chan struct{})
			serverDone := make(chan error, 1)
			var client *http2Client
			t.Cleanup(func() {
				releaseWrite()
				cancel()
				if client != nil {
					client.Close(context.Canceled)
				}
				raw.Close()
				server.Close()
				close(stopServer)
				select {
				case err := <-serverDone:
					if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) && !errors.Is(err, net.ErrClosed) {
						t.Errorf("mock server: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("mock server did not exit")
				}
			})
			go func() {
				serverDone <- serveWriterExitTest(server, ackRead, sendPing, stopServer)
			}()

			var closeCalls atomic.Int32
			tr, err := NewClientTransport(ctx, conn, ConnectOptions{WriteBufferSize: 1},
				"writer-exit-test", nil, func() { closeCalls.Add(1) })
			if err != nil {
				t.Fatalf("create transport: %v", err)
			}
			client = tr.(*http2Client)
			waitDone := func(done <-chan struct{}, what string) {
				t.Helper()
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatalf("timed out waiting for %s", what)
				}
			}
			waitDone(ackRead, "SETTINGS ACK")
			conn.failWrites.Store(true)
			stream, err := client.NewStream(ctx, &CallHdr{Host: "localhost", Method: "/test.Service/WriterExit"})
			if err != nil {
				t.Fatalf("create stream: %v", err)
			}
			waitDone(conn.writeStarted, "injected write failure")

			if tc.throttleReader {
				// With loopy held in Write, fill the response queue. The server's
				// PING makes reader enqueue an ACK and enter throttle before its
				// next ReadFrame. Closing only the connection cannot unblock it.
				for i := 0; i < maxQueuedTransportResponseFrames; i++ {
					if err := client.controlBuf.put(&ping{}); err != nil {
						t.Fatal(err)
					}
				}
				close(sendPing)
				waitWhileTrue(t, func() (bool, error) {
					client.controlBuf.mu.Lock()
					queued := client.controlBuf.transportResponseFrames
					client.controlBuf.mu.Unlock()
					return queued <= maxQueuedTransportResponseFrames, errors.New("reader did not enqueue PING ACK")
				})
			}

			// An existing sender must be released by transport shutdown, without
			// depending on a business timeout or cancellation of the stream.
			if err := client.Write(stream, nil, make([]byte, defaultWriteQuota), &Options{}); err != nil {
				t.Fatalf("consume write quota: %v", err)
			}
			writeDone := make(chan error, 1)
			go func() { writeDone <- client.Write(stream, nil, []byte{1}, &Options{}) }()
			select {
			case err := <-writeDone:
				t.Fatalf("write returned before writer exit: %v", err)
			case <-time.After(10 * time.Millisecond):
			}

			if tc.closeReader {
				go server.Close()
			}
			releaseWrite()
			waitDone(client.writerDone, "writer exit")
			if client.IsActive() {
				t.Error("transport remained active after writer exit")
			}
			if _, err := client.NewStream(ctx, &CallHdr{}); err == nil {
				t.Error("transport accepted a stream after writer exit")
			}
			select {
			case err := <-writeDone:
				if err == nil {
					t.Error("quota-blocked write succeeded after transport shutdown")
				}
			case <-ctx.Done():
				t.Fatal("transport shutdown did not release quota-blocked write")
			}
			waitDone(stream.done, "stream close")
			waitDone(client.readerDone, "reader exit")
			if n := closeCalls.Load(); n != 1 {
				t.Errorf("onClose called %d times, want 1", n)
			}
		})
	}
}

func serveWriterExitTest(conn net.Conn, ackRead chan struct{}, sendPing, stop <-chan struct{}) error {
	if _, err := io.ReadFull(conn, make([]byte, len(ClientPreface))); err != nil {
		return err
	}
	framer := http2.NewFramer(conn, conn)
	frame, err := framer.ReadFrame()
	if err != nil {
		return err
	}
	if _, ok := frame.(*http2.SettingsFrame); !ok {
		return fmt.Errorf("expected client SETTINGS, got %T", frame)
	}
	if err := framer.WriteSettings(); err != nil {
		return err
	}
	frame, err = framer.ReadFrame()
	if err != nil {
		return err
	}
	if settings, ok := frame.(*http2.SettingsFrame); !ok || !settings.IsAck() {
		return fmt.Errorf("expected SETTINGS ACK, got %v", frame)
	}
	close(ackRead)
	select {
	case <-sendPing:
		return framer.WritePing(false, [8]byte{})
	case <-stop:
		return nil
	}
}
