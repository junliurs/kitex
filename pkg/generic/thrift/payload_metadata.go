package thrift

import (
	"context"
	"fmt"

	wire "github.com/cloudwego/gopkg/protocol/thrift"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
)

// PayloadMetadataField locates the metadata struct in the normal unary args
// or result wrapper. It follows the same descriptor snapshot as the codec.
type PayloadMetadataField struct {
	ContainerID, FieldID int16
	Name                 string
	Members              map[int16]string
	Owner, Type          *descriptor.TypeDescriptor
}

func payloadMetadataField(svc *descriptor.ServiceDescriptor, method string, response bool) (PayloadMetadataField, bool, error) {
	fn, err := svc.LookupFunctionByMethod(method)
	if err != nil {
		return PayloadMetadataField{}, false, err
	}
	if fn.IsWithoutWrapping {
		return PayloadMetadataField{}, false, fmt.Errorf("unwrapped metadata is unsupported")
	}
	root := fn.Request
	if response {
		root = fn.Response
	}
	if root == nil || root.Struct == nil {
		return PayloadMetadataField{}, false, nil
	}
	for id, container := range root.Struct.FieldsByID {
		if response && id != 0 {
			continue
		} // Exceptions are not successful business results.
		if container.Type == nil || container.Type.Struct == nil {
			continue
		}
		for fieldID, field := range container.Type.Struct.FieldsByID {
			if (!response && field.Type.IsRequestBase) || (response && field.Type.Name == "base.BaseResp") {
				members := make(map[int16]string, len(field.Type.Struct.FieldsByID))
				for memberID, member := range field.Type.Struct.FieldsByID {
					members[int16(memberID)] = member.FieldName()
				}
				return PayloadMetadataField{ContainerID: int16(id), FieldID: int16(fieldID), Name: field.FieldName(), Members: members, Owner: container.Type, Type: field.Type}, true, nil
			}
		}
	}
	return PayloadMetadataField{}, false, nil
}

func (m *WriteStruct) PayloadMetadataField(method string, response bool) (PayloadMetadataField, bool, error) {
	return payloadMetadataField(m.svcDsc, method, response)
}

func (m *WriteJSON) PayloadMetadataField(method string, response bool) (PayloadMetadataField, bool, error) {
	return payloadMetadataField(m.svcDsc, method, response)
}

// ReadPayloadMetadata borrows only the actual metadata bytes. Decoder-created
// default fields cannot turn an absent field into received authority. The
// business decoder consumes its original reader exactly once.
func ReadPayloadMetadata(body []byte, field PayloadMetadataField) (value []byte, present bool, err error) {
	defer func() {
		if p := recover(); p != nil {
			value, present, err = nil, false, fmt.Errorf("invalid payload metadata: %v", p)
		}
	}()
	container, present, err := payloadStructField(body, field.ContainerID)
	if err != nil || !present {
		return nil, false, err
	}
	return payloadStructField(container, field.FieldID)
}

func payloadStructField(body []byte, wanted int16) ([]byte, bool, error) {
	var found []byte
	present := false
	for at := 0; at < len(body); {
		kind, id, n, err := wire.Binary.ReadFieldBegin(body[at:])
		if err != nil {
			return nil, false, err
		}
		at += n
		if kind == wire.STOP {
			return found, present, nil
		}
		size, err := wire.Binary.Skip(body[at:], kind)
		if err != nil {
			return nil, false, err
		}
		if id == wanted {
			if kind != wire.STRUCT {
				return nil, false, fmt.Errorf("metadata field %d is not a struct", id)
			}
			found, present = body[at:at+size], true
		}
		at += size
	}
	return nil, false, fmt.Errorf("unterminated metadata container")
}

// WithPayloadMetadataEnabled enables IDL metadata projection during decoding.
type payloadMetadataEnabledKey struct{}

func WithPayloadMetadataEnabled(ctx context.Context) context.Context {
	return context.WithValue(ctx, payloadMetadataEnabledKey{}, true)
}
func PayloadMetadataEnabled(ctx context.Context) bool {
	enabled, _ := ctx.Value(payloadMetadataEnabledKey{}).(bool)
	return enabled
}
