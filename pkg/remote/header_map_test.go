package remote

import (
	"context"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"
	"github.com/cloudwego/gopkg/protocol/ttheader"
)

func TestMapPublicAliasesAndWire(t *testing.T) {
	ti := newTransInfo().(*transInfo)
	h := AccessHeaders(ti)
	ints, strs := ti.TransIntInfo(), ti.TransStrInfo()

	ints[9], ints[65535] = "Echo", "unknown\x00integer"
	strs["keep"], strs["delete"], strs["empty"] = "before", "value", ""
	h.SetString("keep", "through-framework")
	if strs["keep"] != "through-framework" {
		t.Fatal("setter replaced public map")
	}
	strs["keep"] = strings.Clone("late\x00value")
	delete(strs, "delete")
	strs["late"] = "inserted"
	if h.String("keep") != "late\x00value" {
		t.Fatal("late alias write was lost")
	}
	if _, found := h.StringValue("delete"); found {
		t.Fatal("late alias delete was lost")
	}
	if v, found := h.StringValue("empty"); !found || v != "" {
		t.Fatal("empty became missing")
	}
	var wire []byte
	w := bufiox.NewBytesWriter(&wire)
	length, err := h.EncodeTTHeader(context.Background(), ttheader.EncodeParam{ProtocolID: ttheader.ProtocolIDDefault, SeqID: 17}, w)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	binary.BigEndian.PutUint32(length, uint32(len(wire)-4))
	got, err := ttheader.Decode(context.Background(), bufiox.NewBytesReader(wire))
	if err != nil {
		t.Fatal(err)
	}
	if got.SeqID != 17 || !reflect.DeepEqual(got.IntInfo, ints) || !reflect.DeepEqual(got.StrInfo, strs) {
		t.Fatalf("wire did not use current maps: %+v", got)
	}
	h.DeleteInt(65535)
	h.DeleteString("late")
	if _, ok := ints[65535]; ok {
		t.Fatal("integer delete did not reach alias")
	}
	if _, ok := strs["late"]; ok {
		t.Fatal("string delete did not reach alias")
	}
	ti.zero()
	if len(ints) != 0 || len(strs) != 0 {
		t.Fatal("recycle retained field roots")
	}
}

func TestMapPutAliasAndMerge(t *testing.T) {
	ti := newTransInfo().(*transInfo)
	ints := map[uint16]string{9: "Echo"}
	strs := map[string]string{"business": "before"}
	ti.PutTransIntInfo(ints)
	ti.PutTransStrInfo(strs)
	ints[9], strs["business"] = "Late", "after"
	h := AccessHeaders(ti)
	if h.Int(9) != "Late" || h.String("business") != "after" {
		t.Fatal("Put aliases detached")
	}
	ti.PutTransIntInfo(map[uint16]string{65535: "new"})
	ti.PutTransStrInfo(map[string]string{"another": "new"})
	if ints[65535] != "new" || strs["another"] != "new" {
		t.Fatal("nonempty Put changed map identity")
	}
	count := 0
	h.RangeString(func(string, string) bool { count++; return false })
	if count != 1 {
		t.Fatal("range ignored early stop")
	}
}

func TestMapBytesResetKeepsAliases(t *testing.T) {
	msg := NewMessage(nil, nil, Call, Server)
	defer RecycleMessage(msg)
	ints, strs := msg.TransInfo().TransIntInfo(), msg.TransInfo().TransStrInfo()
	ints[9], strs["old"] = "old", "old"
	ResetDecodedHeaders(msg)
	if len(ints) != 0 || len(strs) != 0 {
		t.Fatal("byte replacement retained old headers")
	}
	h := AccessHeaders(msg.TransInfo())
	h.SetInt(9, "new")
	h.SetString("new", "new")
	if ints[9] != "new" || strs["new"] != "new" {
		t.Fatal("byte replacement detached public maps")
	}
}
