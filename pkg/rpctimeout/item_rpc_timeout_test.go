package rpctimeout

import (
	"encoding/json"
	"testing"

	"github.com/cloudwego/kitex/internal/test"
)

func TestRPCTimeoutIgnoresStreamTimeoutFields(t *testing.T) {
	cfg := &RPCTimeout{}
	err := json.Unmarshal([]byte(`{
		"rpc_timeout_ms": 1000,
		"conn_timeout_ms": 50,
		"read_timeout_ms": 2000,
		"write_timeout_ms": 3000
	}`), cfg)
	test.Assert(t, err == nil, err)
	test.Assert(t, cfg.RPCTimeoutMS == 1000, cfg)
	test.Assert(t, cfg.ConnTimeoutMS == 50, cfg)

	copied := cfg.DeepCopy().(*RPCTimeout)
	test.Assert(t, copied != cfg)
	test.Assert(t, copied.EqualsTo(cfg))

	copied.RPCTimeoutMS++
	test.Assert(t, !copied.EqualsTo(cfg))
}
