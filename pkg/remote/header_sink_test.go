package remote

import (
	"reflect"
	"testing"

	"github.com/cloudwego/gopkg/protocol/ttheader"
)

func TestHeaderSinkPreservesOpaqueFields(t *testing.T) {
	for _, stringFirst := range []bool{false, true} {
		direct, accessed := newTransInfo().(*transInfo), newTransInfo().(*transInfo)
		a, b := AccessHeaders(direct), AccessHeaders(accessed)

		direct.SetInt(65535, "opaque\x00value")
		b.SetInt(65535, "opaque\x00value")
		direct.SetString("", "")
		b.SetString("", "")
		if stringFirst {
			direct.SetString(ttheader.GDPRToken, "string")
			b.SetString(ttheader.GDPRToken, "string")
		} else {
			direct.SetInt(32, "integer")
			b.SetInt(32, "integer")
		}

		direct.SetInt(32, "integer")
		b.SetInt(32, "integer")
		direct.SetString(ttheader.GDPRToken, "string")
		b.SetString(ttheader.GDPRToken, "string")

		direct.SetInt(9, "after")
		b.SetInt(9, "after")
		direct.SetString("mct", "after")
		b.SetString("mct", "after")
		ints, strs := a.LegacyMaps()
		wantInts, wantStrs := b.LegacyMaps()
		if !reflect.DeepEqual(ints, wantInts) || !reflect.DeepEqual(strs, wantStrs) {
			t.Fatal("header setters changed opaque fields")
		}
	}
}
