package remote

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"
	"github.com/cloudwego/gopkg/protocol/ttheader"
)

type customHeaderInfo struct {
	ints             map[uint16]string
	strs             map[string]string
	intPuts, strPuts int
}

func (c *customHeaderInfo) TransIntInfo() map[uint16]string { return c.ints }
func (c *customHeaderInfo) TransStrInfo() map[string]string { return c.strs }
func (c *customHeaderInfo) PutTransIntInfo(values map[uint16]string) {
	if values == nil {
		return
	}
	c.intPuts++
	if len(c.ints) == 0 {
		c.ints = values
		return
	}
	for key, value := range values {
		c.ints[key] = value
	}
}

func (c *customHeaderInfo) PutTransStrInfo(values map[string]string) {
	if values == nil {
		return
	}
	c.strPuts++
	if len(c.strs) == 0 {
		c.strs = values
		return
	}
	for key, value := range values {
		c.strs[key] = value
	}
}
func (c *customHeaderInfo) Recycle() { c.ints, c.strs = nil, nil }

func TestHeaderCustomAndNil(t *testing.T) {
	empty := AccessHeaders(nil)

	empty.SetInt(9, "unused")
	empty.SetString("unused", "value")
	empty.DeleteInt(9)
	empty.DeleteString("unused")
	if _, ok := empty.IntValue(9); ok {
		t.Fatal("nil lookup")
	}
	if _, ok := empty.StringValue("unused"); ok {
		t.Fatal("nil lookup")
	}
	empty.RangeInt(func(uint16, string) bool { t.Fatal("nil range"); return true })
	empty.RangeString(func(string, string) bool { t.Fatal("nil range"); return true })
	custom := &customHeaderInfo{}
	h := AccessHeaders(custom)

	wire := headerTestWire(t, map[uint16]string{65535: "unknown"}, map[string]string{"": "empty"})
	if _, err := h.DecodeTTHeader(context.Background(), bufiox.NewBytesReader(wire)); err != nil {
		t.Fatal(err)
	}
	if h.Int(65535) != "unknown" || h.String("") != "empty" {
		t.Fatal("nil map return was not installed")
	}
	h.SetInt(9, "Echo")
	h.SetString("new", "value")
	h.DeleteInt(65535)
	h.DeleteString("")
	ints, strs := collectHeaders(h)
	if len(ints) != 1 || ints[9] != "Echo" || len(strs) != 1 || strs["new"] != "value" {
		t.Fatal("custom safe access")
	}
}

func TestHeaderCustomSetPreservesDirectMapContract(t *testing.T) {
	for _, allocated := range []bool{false, true} {
		custom := &customHeaderInfo{}
		if allocated {
			custom.ints, custom.strs = map[uint16]string{}, map[string]string{}
		}
		intAlias, strAlias := custom.ints, custom.strs
		h := AccessHeaders(custom)
		h.SetInt(9, "Echo")
		h.SetString("key", "value")
		if allocated {
			if custom.intPuts != 0 || custom.strPuts != 0 || intAlias[9] != "Echo" || strAlias["key"] != "value" {
				t.Fatal("custom direct map became Put")
			}
		} else if custom.intPuts != 1 || custom.strPuts != 1 {
			t.Fatal("nil map did not use Put")
		}
		intPuts, strPuts := custom.intPuts, custom.strPuts
		h.SetInt(9, "Changed")
		h.SetString("key", "changed")
		h.DeleteInt(9)
		h.DeleteString("key")
		if custom.intPuts != intPuts || custom.strPuts != strPuts || len(custom.ints) != 0 || len(custom.strs) != 0 {
			t.Fatal("custom direct update/delete")
		}
	}
}

func collectHeaders(h HeaderAccess) (map[uint16]string, map[string]string) {
	ints, strs := make(map[uint16]string), make(map[string]string)
	h.RangeInt(func(key uint16, value string) bool { ints[key] = value; return true })
	h.RangeString(func(key, value string) bool { strs[key] = value; return true })
	return ints, strs
}

func headerTestWire(t *testing.T, ints map[uint16]string, strs map[string]string) []byte {
	t.Helper()
	var wire []byte
	out := bufiox.NewBytesWriter(&wire)
	field, err := ttheader.Encode(context.Background(), ttheader.EncodeParam{ProtocolID: ttheader.ProtocolIDThriftBinary, IntInfo: ints, StrInfo: strs}, out)
	if err != nil {
		t.Fatal(err)
	}
	if err = out.Flush(); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(field, uint32(len(wire)-4))
	return wire
}
