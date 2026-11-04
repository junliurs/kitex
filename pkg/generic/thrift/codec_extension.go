package thrift

import (
	"context"

	wire "github.com/cloudwego/gopkg/protocol/thrift"
	"github.com/cloudwego/kitex/pkg/generic/descriptor"
)

// FieldCodec extends structured decoding and encoding without selecting fields
// or interpreting their values. ReadField sees received values before mappings;
// ReadValue sees the value that will be assigned to the containing struct.
type FieldCodec interface {
	// BeginStruct may request string keys for nested maps in this struct.
	BeginStruct(*descriptor.TypeDescriptor) bool
	ReadField(*descriptor.TypeDescriptor, *descriptor.FieldDescriptor, interface{}) error
	ReadValue(*descriptor.TypeDescriptor, *descriptor.FieldDescriptor, interface{}) error
	WriteField(context.Context, *wire.BufferWriter, *descriptor.TypeDescriptor, *descriptor.FieldDescriptor) (bool, error)
}

// CodecOptions explicitly selects the structured JSON backend when field hooks
// are needed. JSONResult may supply a caller-owned representation of its result.
type CodecOptions struct {
	Fields         FieldCodec
	StructuredJSON bool
	JSONResult     func(interface{}) (interface{}, error)
}

type codecOptionsKey struct{}

func WithCodecOptions(ctx context.Context, options *CodecOptions) context.Context {
	return context.WithValue(ctx, codecOptionsKey{}, options)
}

func codecOptions(ctx context.Context) *CodecOptions {
	options, _ := ctx.Value(codecOptionsKey{}).(*CodecOptions)
	return options
}

func fieldCodec(ctx context.Context) FieldCodec {
	if options := codecOptions(ctx); options != nil {
		return options.Fields
	}
	return nil
}

func structuredJSON(ctx context.Context) bool {
	options := codecOptions(ctx)
	return options != nil && options.StructuredJSON
}

func writeCodecField(ctx context.Context, out *wire.BufferWriter, owner *descriptor.TypeDescriptor, field *descriptor.FieldDescriptor) (bool, error) {
	if codec := fieldCodec(ctx); codec != nil {
		return codec.WriteField(ctx, out, owner, field)
	}
	return false, nil
}

// MessageCodecExtension owns optional per-message transformation state. The
// generic message forwards values; the extension chooses their representation.
type MessageCodecExtension interface {
	CodecContext(context.Context) context.Context
	Decoded(interface{}) (interface{}, error)
	Materialize(interface{}) (interface{}, error)
}

type MessageCodecExtensionFactory interface {
	NewMessageCodecExtension(codec interface{}, method string, response bool) (MessageCodecExtension, error)
}

type messageCodecExtensionKey struct{}

func WithMessageCodecExtensionFactory(ctx context.Context, factory MessageCodecExtensionFactory) context.Context {
	return context.WithValue(ctx, messageCodecExtensionKey{}, factory)
}

func MessageCodecExtensionFactoryFromContext(ctx context.Context) MessageCodecExtensionFactory {
	factory, _ := ctx.Value(messageCodecExtensionKey{}).(MessageCodecExtensionFactory)
	return factory
}

// ServiceDescriptor exposes the same descriptor snapshot used by this writer.
func (m *WriteStruct) ServiceDescriptor() *descriptor.ServiceDescriptor { return m.svcDsc }
func (m *WriteJSON) ServiceDescriptor() *descriptor.ServiceDescriptor   { return m.svcDsc }
