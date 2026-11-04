package netpoll

import (
	"net"
	"testing"
	"time"

	"github.com/cloudwego/kitex/pkg/remote"
)

func TestShutdownBeforeBootstrap(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	s := &transServer{opt: &remote.ServerOption{ExitWaitTime: time.Second}}
	if err = s.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err = s.BootstrapServer(ln); err != nil {
		t.Fatal(err)
	}
	if _, err = ln.Accept(); err == nil {
		t.Fatal("stopped server left its listener active")
	}
}
