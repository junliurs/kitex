package generic

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"
	wire "github.com/cloudwego/gopkg/protocol/thrift"
	"github.com/cloudwego/gopkg/protocol/thrift/base"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	genericthrift "github.com/cloudwego/kitex/pkg/generic/thrift"
	"github.com/cloudwego/kitex/pkg/remote"
)

func metadataDescriptor() *descriptor.ServiceDescriptor {
	text := &descriptor.TypeDescriptor{Type: descriptor.STRING, Name: "string"}
	field := func(id int32, name string, ty *descriptor.TypeDescriptor) *descriptor.FieldDescriptor {
		return &descriptor.FieldDescriptor{ID: id, Name: name, Type: ty}
	}
	structure := func(name string, fields ...*descriptor.FieldDescriptor) *descriptor.TypeDescriptor {
		s := &descriptor.TypeDescriptor{Type: descriptor.STRUCT, Name: name, Struct: &descriptor.StructDescriptor{FieldsByID: map[int32]*descriptor.FieldDescriptor{}, FieldsByName: map[string]*descriptor.FieldDescriptor{}}}
		for _, f := range fields {
			s.Struct.FieldsByID[f.ID] = f
			s.Struct.FieldsByName[f.Name] = f
		}
		return s
	}
	extra := &descriptor.TypeDescriptor{Type: descriptor.MAP, Key: text, Elem: text}
	requestBase := structure("base.Base", field(1, "trace", text), field(2, "caller", text), field(3, "address", text), field(6, "attributes", extra))
	requestBase.IsRequestBase = true
	requestField := field(201, "Context", requestBase)
	req := structure("Request", requestField)
	// A decoder default deliberately resembles a real received identity.
	req.Struct.DefaultFields = map[string]*descriptor.FieldDescriptor{"Context": {ID: 201, Name: "Context", Type: requestBase, DefaultValue: map[string]any{"caller": "fabricated"}}}
	responseBase := structure("base.BaseResp", field(1, "description", text), field(2, "code", &descriptor.TypeDescriptor{Type: descriptor.I32}), field(3, "attributes", extra))
	resp := structure("Response", field(202, "ResponseContext", responseBase))
	return &descriptor.ServiceDescriptor{Name: "Metadata", Functions: map[string]*descriptor.FunctionDescriptor{"Visit": {Name: "Visit", HasRequestBase: true, Request: structure("Args", field(7, "request", req)), Response: structure("Result", field(0, "success", resp))}}}
}

func metadataFieldBytes(id int16, value []byte) []byte {
	out := []byte{byte(wire.STRUCT), byte(uint16(id) >> 8), byte(id)}
	out = append(out, value...)
	return append(out, 0)
}

func encodedMetadataBase() *base.Base {
	return &base.Base{Caller: "wire-caller", LogID: "wire-log", Addr: "192.0.2.1", Extra: map[string]string{"keep": "wire-value"}}
}

func requestMetadataBytes(present bool) []byte {
	req := []byte{0}
	if present {
		b := encodedMetadataBase()
		raw := make([]byte, b.BLength())
		b.FastWrite(raw)
		req = metadataFieldBytes(201, raw)
	}
	return metadataFieldBytes(7, req)
}

func metadataContext(response bool) (context.Context, *remote.UnaryCall) {
	c := remote.NewUnaryCall(nil)
	if response {
		c.ResponseWire = remote.UnaryWire{Framed: true}
	} else {
		c.RequestWire = remote.UnaryWire{Framed: true}
	}
	return remote.WithUnaryCall(context.Background(), c), c
}

func TestUnaryGenericMetadataReadsActualFieldAndOwnsStrings(t *testing.T) {
	a := &Args{}
	a.SetCodec(genericthrift.NewStructReaderWriter(metadataDescriptor()))
	raw := requestMetadataBytes(true)
	in := bufiox.NewBytesReader(raw)
	defer in.Release(nil)
	ctx, c := metadataContext(false)
	defer c.Recycle()
	if err := a.Read(ctx, "Visit", len(raw), in); err != nil {
		t.Fatal(err)
	}
	field, b, received, err := a.RequestBaseMetadata("Visit")
	if err != nil || !received || field.Name != "Context" || field.Members[2] != "caller" || b == nil {
		t.Fatal(field, b, received, err)
	}
	for i := range raw {
		raw[i] = 0xcc
	}
	if b.Caller != "wire-caller" || b.LogID != "wire-log" || b.Extra["keep"] != "wire-value" {
		t.Fatal("borrowed released wire", b)
	}
	if in.ReadLen() != len(raw) {
		t.Fatal("metadata changed codec consumption", in.ReadLen())
	}
}

func TestUnaryGenericMetadataAbsenceOverridesDecoderDefaultsAndPreviousCall(t *testing.T) {
	a := &Args{}
	a.SetCodec(genericthrift.NewStructReaderWriter(metadataDescriptor()))
	ctx, c := metadataContext(false)
	defer c.Recycle()
	for _, present := range []bool{true, false} {
		raw := requestMetadataBytes(present)
		in := bufiox.NewBytesReader(raw)
		if err := a.Read(ctx, "Visit", len(raw), in); err != nil {
			t.Fatal(err)
		}
		_, b, received, err := a.RequestBaseMetadata("Visit")
		if err != nil || !received || (b != nil) != present {
			t.Fatal("fake or stale Base", b, present, err)
		}
		_ = in.Release(nil)
	}
	body := a.Request.(map[string]any)
	if body["Context"] == nil {
		t.Fatal("test did not create a decoder default")
	}
}

func TestUnaryGenericResponseMetadataKeepsBusinessCodeAndTracksAbsence(t *testing.T) {
	result := &Result{}
	result.SetCodec(genericthrift.NewStructReaderWriter(metadataDescriptor()))
	ctx, c := metadataContext(true)
	defer c.Recycle()
	for _, present := range []bool{true, false} {
		response := []byte{0}
		if present {
			b := &base.BaseResp{StatusCode: 1606, StatusMessage: "business", Extra: map[string]string{"sc": "0"}}
			raw := make([]byte, b.BLength())
			b.FastWrite(raw)
			response = metadataFieldBytes(202, raw)
		}
		raw := metadataFieldBytes(0, response)
		in := bufiox.NewBytesReader(raw)
		if err := result.Read(ctx, "Visit", len(raw), in); err != nil {
			t.Fatal(err)
		}
		field, b, received, err := result.ResponseBaseMetadata("Visit")
		if err != nil || !received || field.Name != "ResponseContext" || (b != nil) != present {
			t.Fatal(field, b, received, err)
		}
		if b != nil && (b.StatusCode != 1606 || b.StatusMessage != "business" || b.Extra["sc"] != "0") {
			t.Fatal("business status changed", b)
		}
		_ = in.Release(nil)
	}
}

func TestUnaryGenericMetadataMalformedWireDoesNotBecomeNone(t *testing.T) {
	field, _, err := genericthrift.NewWriteStruct(metadataDescriptor()).PayloadMetadataField("Visit", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range [][]byte{{12, 0, 7, 12, 0, 201}, {0xff, 0, 7, 0}, {11, 0, 7, 0, 0, 0, 0, 0}} {
		if _, _, err := genericthrift.ReadPayloadMetadata(raw, field); err == nil {
			t.Fatal("malformed metadata accepted", raw)
		}
	}
}

func TestUnaryGenericMetadataUnknownCodecFailsExplicitly(t *testing.T) {
	a := &Args{}
	a.SetCodec(struct{}{})
	if _, _, _, err := a.RequestBaseMetadata("Visit"); err == nil {
		t.Fatal("unknown metadata codec silently became NONE")
	}
}

type noMetadataRescan struct {
	bufiox.Reader
	whole int
	scans int
}

func (r *noMetadataRescan) Peek(n int) ([]byte, error) {
	if n == r.whole {
		r.scans++
		return nil, fmt.Errorf("unexpected full-body metadata scan")
	}
	return r.Reader.Peek(n)
}

func TestUnaryGenericMetadataSharesCodecTraversalAndDefersJSON(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		a := &Args{}
		if jsonMode {
			a.SetCodec(genericthrift.NewJsonReaderWriter(metadataDescriptor()))
		} else {
			a.SetCodec(genericthrift.NewStructReaderWriter(metadataDescriptor()))
		}
		raw := requestMetadataBytes(true)
		reader := &noMetadataRescan{Reader: bufiox.NewBytesReader(raw), whole: len(raw)}
		defer reader.Release(nil)
		ctx, c := metadataContext(false)
		c.RequestWire.Framed = false
		if err := a.Read(ctx, "Visit", 0, reader); err != nil {
			t.Fatal(err)
		}
		_, b, received, err := a.RequestBaseMetadata("Visit")
		if err != nil || !received || b == nil || b.Caller != "wire-caller" || reader.scans != 0 {
			t.Fatal(b, received, err, reader.scans)
		}
		b.Caller = "governed"
		a.SetRequestBaseMetadata(b)
		if jsonMode {
			if _, already := a.Request.(string); already {
				t.Fatal("JSON was materialized before governance")
			}
			if err := a.MaterializePayload(); err != nil {
				t.Fatal(err)
			}
			text := a.Request.(string)
			if !strings.Contains(text, "governed") {
				t.Fatal(text)
			}
			if err := a.MaterializePayload(); err != nil || a.Request.(string) != text {
				t.Fatal("materialized again", err)
			}
		}
		c.Recycle()
	}
}

type mappedMetadataCaller struct{}

func (mappedMetadataCaller) Request(_ context.Context, v interface{}, _ *descriptor.FieldDescriptor) (interface{}, error) {
	return v, nil
}

func (mappedMetadataCaller) Response(_ context.Context, _ interface{}, _ *descriptor.FieldDescriptor) (interface{}, error) {
	return "business-mapping", nil
}

func TestUnaryGenericMetadataAuthorityPrecedesBusinessMappingsAndDefaults(t *testing.T) {
	for _, missingCaller := range []bool{false, true} {
		svc := metadataDescriptor()
		meta := svc.Functions["Visit"].Request.Struct.FieldsByID[7].Type.Struct.FieldsByID[201].Type
		if missingCaller {
			meta.Struct.DefaultFields = map[string]*descriptor.FieldDescriptor{"caller": {ID: 2, Name: "caller", Type: meta.Struct.FieldsByID[2].Type, DefaultValue: "default-caller"}}
		} else {
			meta.Struct.FieldsByID[2].ValueMapping = mappedMetadataCaller{}
		}
		a := &Args{}
		a.SetCodec(genericthrift.NewStructReaderWriter(svc))
		raw := requestMetadataBytes(true)
		if missingCaller {
			raw = metadataFieldBytes(7, metadataFieldBytes(201, []byte{0}))
		}
		in := bufiox.NewBytesReader(raw)
		defer in.Release(nil)
		ctx, c := metadataContext(false)
		defer c.Recycle()
		if err := a.Read(ctx, "Visit", len(raw), in); err != nil {
			t.Fatal(err)
		}
		_, value, _, err := a.RequestBaseMetadata("Visit")
		if err != nil || value == nil {
			t.Fatal(value, err)
		}
		expected := "wire-caller"
		if missingCaller {
			expected = ""
		}
		if value.Caller != expected {
			t.Fatal("business mapping/default became wire authority", value.Caller, expected)
		}
	}
}
