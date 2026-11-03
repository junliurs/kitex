package remote

import (
	"context"
	"unsafe"

	"github.com/cloudwego/gopkg/bufiox"
	"github.com/cloudwego/gopkg/protocol/ttheader"
)

type HeaderEntry = struct {
	Kind, ID   uint16
	Op         uint32
	Key, Value string
}

type HeaderView struct {
	entries       []HeaderEntry
	metadataIndex [21]uint32
	byteSize      int
	valid         bool
}

func (s *HeaderView) Valid() bool { return s != nil && s.valid }
func (s *HeaderView) Len() int {
	if s == nil {
		return 0
	}
	return len(s.entries)
}

func HeaderViewLayout() [6]uintptr {
	return [6]uintptr{unsafe.Sizeof(HeaderView{}), unsafe.Alignof(HeaderView{}), unsafe.Offsetof(HeaderView{}.entries), unsafe.Offsetof(HeaderView{}.metadataIndex), unsafe.Offsetof(HeaderView{}.byteSize), unsafe.Offsetof(HeaderView{}.valid)}
}

// HeaderAccess reads and writes the message's original maps. Exported view
// methods remain compatibility queries and never create another field store.
type HeaderAccess struct{ info TransInfo }

func AccessHeaders(info TransInfo) HeaderAccess     { return HeaderAccess{info: info} }
func (a HeaderAccess) EnablePrimary(bool) bool      { return false }
func (a HeaderAccess) Primary() bool                { return false }
func (a HeaderAccess) View() *HeaderView            { return nil }
func (a HeaderAccess) ResetNativeChanges(bool) bool { return false }
func (a HeaderAccess) NativeChanges() *HeaderView   { return nil }

func (ti *transInfo) materialize() {
	if ti.intInfo == nil {
		ti.intInfo = make(map[uint16]string)
	}
	if ti.strInfo == nil {
		ti.strInfo = make(map[string]string)
	}
}

func (ti *transInfo) SetInt(id uint16, value string) {
	if ti.intInfo == nil {
		ti.intInfo = make(map[uint16]string)
	}
	ti.intInfo[id] = value
}

func (ti *transInfo) SetString(key, value string) {
	if ti.strInfo == nil {
		ti.strInfo = make(map[string]string)
	}
	ti.strInfo[key] = value
}
func (a HeaderAccess) Int(id uint16) string     { v, _ := a.IntValue(id); return v }
func (a HeaderAccess) String(key string) string { v, _ := a.StringValue(key); return v }
func (a HeaderAccess) IntValue(id uint16) (string, bool) {
	if a.info == nil {
		return "", false
	}
	if ti, ok := a.info.(*transInfo); ok {
		v, found := ti.intInfo[id]
		return v, found
	}
	v, found := a.info.TransIntInfo()[id]
	return v, found
}

func (a HeaderAccess) StringValue(key string) (string, bool) {
	if a.info == nil {
		return "", false
	}
	if ti, ok := a.info.(*transInfo); ok {
		v, found := ti.strInfo[key]
		return v, found
	}
	v, found := a.info.TransStrInfo()[key]
	return v, found
}

func (a HeaderAccess) SetInt(id uint16, value string) {
	if a.info == nil {
		return
	}
	if ti, ok := a.info.(*transInfo); ok {
		ti.SetInt(id, value)
		return
	}
	if values := a.info.TransIntInfo(); values != nil {
		values[id] = value
	} else {
		a.info.PutTransIntInfo(map[uint16]string{id: value})
	}
}

func (a HeaderAccess) SetString(key, value string) {
	if a.info == nil {
		return
	}
	if ti, ok := a.info.(*transInfo); ok {
		ti.SetString(key, value)
		return
	}
	if values := a.info.TransStrInfo(); values != nil {
		values[key] = value
	} else {
		a.info.PutTransStrInfo(map[string]string{key: value})
	}
}

func (a HeaderAccess) DeleteInt(id uint16) {
	if a.info == nil {
		return
	}
	if ti, ok := a.info.(*transInfo); ok {
		delete(ti.intInfo, id)
		return
	}
	delete(a.info.TransIntInfo(), id)
}

func (a HeaderAccess) DeleteString(key string) {
	if a.info == nil {
		return
	}
	if ti, ok := a.info.(*transInfo); ok {
		delete(ti.strInfo, key)
		return
	}
	delete(a.info.TransStrInfo(), key)
}

func (a HeaderAccess) MergeInt(values map[uint16]string) {
	for k, v := range values {
		a.SetInt(k, v)
	}
}

func (a HeaderAccess) MergeString(values map[string]string) {
	for k, v := range values {
		a.SetString(k, v)
	}
}

func (a HeaderAccess) RangeInt(visit func(uint16, string) bool) {
	if a.info == nil {
		return
	}
	var values map[uint16]string
	if ti, ok := a.info.(*transInfo); ok {
		values = ti.intInfo
	} else {
		values = a.info.TransIntInfo()
	}
	for k, v := range values {
		if !visit(k, v) {
			return
		}
	}
}

func (a HeaderAccess) RangeString(visit func(string, string) bool) {
	if a.info == nil {
		return
	}
	var values map[string]string
	if ti, ok := a.info.(*transInfo); ok {
		values = ti.strInfo
	} else {
		values = a.info.TransStrInfo()
	}
	for k, v := range values {
		if !visit(k, v) {
			return
		}
	}
}

func (a HeaderAccess) LegacyMaps() (map[uint16]string, map[string]string) {
	if a.info == nil {
		return nil, nil
	}
	return a.info.TransIntInfo(), a.info.TransStrInfo()
}

func (a HeaderAccess) EncodeTTHeader(ctx context.Context, p ttheader.EncodeParam, out bufiox.Writer) ([]byte, error) {
	if ti, ok := a.info.(*transInfo); ok {
		p.IntInfo, p.StrInfo = ti.intInfo, ti.strInfo
	} else {
		p.IntInfo, p.StrInfo = a.LegacyMaps()
	}
	return ttheader.Encode(ctx, p, out)
}

func (a HeaderAccess) DecodeTTHeader(ctx context.Context, in bufiox.Reader) (ttheader.DecodeParam, error) {
	var p ttheader.DecodeParam
	var err error
	intInfo, strInfo := a.LegacyMaps()
	reuse := len(intInfo) == 0 && len(strInfo) == 0
	if reuse {
		p, err = ttheader.DecodeInto(ctx, in, intInfo, strInfo)
	} else {
		p, err = ttheader.Decode(ctx, in)
	}
	if err != nil {
		if reuse {
			for key := range intInfo {
				delete(intInfo, key)
			}
			for key := range strInfo {
				delete(strInfo, key)
			}
		}
		return p, err
	}
	if a.info != nil {
		if !reuse || intInfo == nil {
			a.info.PutTransIntInfo(p.IntInfo)
		}
		if !reuse || strInfo == nil {
			a.info.PutTransStrInfo(p.StrInfo)
		}
	}
	return p, nil
}

// ResetDecodedHeaders clears decoded transport metadata before decoding a replacement frame.
func ResetDecodedHeaders(msg Message) {
	if ti, ok := msg.TransInfo().(*transInfo); ok {
		for k := range ti.intInfo {
			delete(ti.intInfo, k)
		}
		for k := range ti.strInfo {
			delete(ti.strInfo, k)
		}
	}
}
