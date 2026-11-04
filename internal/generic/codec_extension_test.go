package generic

import (
	"context"
	"errors"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"
	wire "github.com/cloudwego/gopkg/protocol/thrift"
	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	"github.com/cloudwego/kitex/pkg/generic/thrift"
)

type arbitraryFields struct {
	target   *descriptor.TypeDescriptor
	seen     map[int32]interface{}
	writeErr error
}

func (f *arbitraryFields) BeginStruct(t *descriptor.TypeDescriptor) bool { return t == f.target }
func (f *arbitraryFields) ReadField(owner *descriptor.TypeDescriptor, field *descriptor.FieldDescriptor, value interface{}) error {
	if owner == f.target {
		f.seen[field.ID] = value
	}
	return nil
}
func (*arbitraryFields) ReadValue(*descriptor.TypeDescriptor, *descriptor.FieldDescriptor, interface{}) error {
	return nil
}
func (f *arbitraryFields) WriteField(_ context.Context, out *wire.BufferWriter, owner *descriptor.TypeDescriptor, field *descriptor.FieldDescriptor) (bool, error) {
	if owner != f.target || field.ID != 7 {
		return false, nil
	}
	if f.writeErr != nil {
		return false, f.writeErr
	}
	if err := out.WriteFieldBegin(wire.STRING, 7); err != nil {
		return true, err
	}
	return true, out.WriteString("updated")
}

func extensionDescriptor() (*descriptor.ServiceDescriptor, *descriptor.TypeDescriptor) {
	text := &descriptor.TypeDescriptor{Type: descriptor.STRING, Name: "string"}
	field := &descriptor.FieldDescriptor{ID: 7, Name: "Token", Type: text}
	defaultField := &descriptor.FieldDescriptor{ID: 8, Name: "Default", Type: text, DefaultValue: "synthesized"}
	object := &descriptor.TypeDescriptor{Type: descriptor.STRUCT, Name: "example.Authentication", Struct: &descriptor.StructDescriptor{
		FieldsByID:    map[int32]*descriptor.FieldDescriptor{7: field, 8: defaultField},
		FieldsByName:  map[string]*descriptor.FieldDescriptor{"Token": field, "Default": defaultField},
		DefaultFields: map[string]*descriptor.FieldDescriptor{"Default": defaultField},
	}}
	arg := &descriptor.FieldDescriptor{ID: 1, Name: "request", Type: object}
	args := &descriptor.TypeDescriptor{Type: descriptor.STRUCT, Struct: &descriptor.StructDescriptor{
		FieldsByID: map[int32]*descriptor.FieldDescriptor{1: arg}, FieldsByName: map[string]*descriptor.FieldDescriptor{"request": arg},
	}}
	return &descriptor.ServiceDescriptor{Functions: map[string]*descriptor.FunctionDescriptor{"Invoke": {Name: "Invoke", Request: args}}}, object
}

func TestFieldExtensionHandlesArbitraryIDLAndReceivedPresence(t *testing.T) {
	svc, object := extensionDescriptor()
	codec := thrift.NewStructReaderWriter(svc)
	fields := &arbitraryFields{target: object, seen: map[int32]interface{}{}}
	ctx := thrift.WithCodecOptions(context.Background(), &thrift.CodecOptions{Fields: fields})
	// args.request.Token="wire"; Default is absent from the wire.
	data := []byte{12, 0, 1, 11, 0, 7, 0, 0, 0, 4, 'w', 'i', 'r', 'e', 0, 0}
	args := &Args{}
	args.SetCodec(codec)
	in := bufiox.NewBytesReader(data)
	defer in.Release(nil)
	if err := args.Read(ctx, "Invoke", len(data), in); err != nil {
		t.Fatal(err)
	}
	if fields.seen[7] != "wire" || len(fields.seen) != 1 {
		t.Fatal(fields.seen)
	}
	if args.Request.(map[string]interface{})["Default"] != "synthesized" {
		t.Fatal(args.Request)
	}
	var encoded []byte
	out := bufiox.NewBytesWriter(&encoded)
	if err := args.Write(ctx, "Invoke", out); err != nil {
		t.Fatal(err)
	}
	if err := out.Flush(); err != nil {
		t.Fatal(err)
	}
	other := &Args{}
	other.SetCodec(codec)
	input := bufiox.NewBytesReader(encoded)
	defer input.Release(nil)
	if err := other.Read(context.Background(), "Invoke", len(encoded), input); err != nil {
		t.Fatal(err)
	}
	if other.Request.(map[string]interface{})["Token"] != "updated" {
		t.Fatal(other.Request)
	}
	fields.writeErr = errors.New("field transformation failed")
	var failedOutput []byte
	output := bufiox.NewBytesWriter(&failedOutput)
	if err := args.Write(ctx, "Invoke", output); !errors.Is(err, fields.writeErr) {
		t.Fatalf("field error was ignored: %v", err)
	}
}
