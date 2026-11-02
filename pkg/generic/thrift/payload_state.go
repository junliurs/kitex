package thrift

import (
	"context"
	"fmt"

	wire "github.com/cloudwego/gopkg/protocol/thrift"
	"github.com/cloudwego/gopkg/protocol/thrift/base"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/gjson"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	"github.com/cloudwego/kitex/pkg/utils"
)

// PayloadMetadata is per-message metadata produced by the existing codec.
// It never holds a second copy of a business payload.
type PayloadMetadata struct {
	Field           PayloadMetadataField
	Request         *base.Base
	Response        *base.BaseResp
	Received        bool
	Object          map[string]interface{}
	pendingRequest  *base.Base
	pendingResponse *base.BaseResp
	JSON            *PayloadJSON
}
type payloadMetadataKey struct{}

func WithPayloadMetadata(ctx context.Context, metadata *PayloadMetadata) context.Context {
	if metadata == nil || metadata.Field.Name == "" {
		return ctx
	}
	return context.WithValue(ctx, payloadMetadataKey{}, metadata)
}

func payloadMetadataFromContext(ctx context.Context) *PayloadMetadata {
	m, _ := ctx.Value(payloadMetadataKey{}).(*PayloadMetadata)
	return m
}

// PayloadJSON defers the codec's normal JSON materialization until governance
// has updated the decoded metadata object. Business data is decoded only once.
type PayloadJSON struct{ Value interface{} }

func (p *PayloadJSON) Materialize() (string, error) {
	b, err := jsoniter.Marshal(p.Value)
	return utils.SliceByteToString(b), err
}

func metadataString(value interface{}) (string, error) {
	if value == nil {
		return "", nil
	}
	if n, ok := value.(gjson.Result); ok {
		if !n.Exists() || n.Type == gjson.Null {
			return "", nil
		}
		if n.Type != gjson.String {
			return "", fmt.Errorf("metadata field is not a string")
		}
		return n.String(), nil
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("metadata field is not a string")
	}
	return s, nil
}

func metadataExtra(value interface{}, relaxed bool) (map[string]string, error) {
	if value == nil {
		return nil, nil
	}
	out := map[string]string{}
	put := func(k string, v interface{}) error {
		s, err := metadataString(v)
		if err != nil && relaxed {
			return nil
		}
		if err != nil {
			return err
		}
		out[k] = s
		return nil
	}
	switch v := value.(type) {
	case map[string]string:
		for k, x := range v {
			out[k] = x
		}
	case map[string]interface{}:
		for k, x := range v {
			if err := put(k, x); err != nil {
				return nil, err
			}
		}
	case map[interface{}]interface{}:
		for k, x := range v {
			s, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("metadata extra key is not a string")
			}
			if err := put(s, x); err != nil {
				return nil, err
			}
		}
	case gjson.Result:
		if !v.Exists() || v.Type == gjson.Null {
			return nil, nil
		}
		if !v.IsObject() {
			return nil, fmt.Errorf("metadata extra is not an object")
		}
		var err error
		v.ForEach(func(k, x gjson.Result) bool { err = put(k.String(), x); return err == nil })
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported metadata extra %T", value)
	}
	return out, nil
}

func (m *PayloadMetadata) values(get func(int16) interface{}, response bool, cached *base.Base) error {
	if response {
		b := new(base.BaseResp)
		var err error
		if b.StatusMessage, err = metadataString(get(1)); err != nil {
			return err
		}
		switch code := get(2).(type) {
		case nil:
		case int32:
			b.StatusCode = code
		case int64:
			b.StatusCode = int32(code)
		case int:
			b.StatusCode = int32(code)
		case gjson.Result:
			b.StatusCode = int32(code.Int())
		default:
			return fmt.Errorf("invalid BaseResp status %T", code)
		}
		if b.Extra, err = metadataExtra(get(3), false); err != nil {
			return err
		}
		m.Response = b
		return nil
	}
	b := cached
	if b == nil {
		b = new(base.Base)
		var err error
		if b.LogID, err = metadataString(get(1)); err != nil {
			return err
		}
		if b.Caller, err = metadataString(get(2)); err != nil {
			return err
		}
		if b.Addr, err = metadataString(get(3)); err != nil {
			return err
		}
	}
	extra, err := metadataExtra(get(6), cached != nil)
	if err != nil {
		return err
	}
	if len(extra) > 0 && b.Extra == nil {
		b.Extra = map[string]string{}
	}
	for k, v := range extra {
		if _, exists := b.Extra[k]; !exists {
			b.Extra[k] = v
		}
	}
	m.Request = b
	return nil
}

// Record members before business value mappings can alter a decoded object.
// Defaulted fields never enter this path and cannot create caller authority.
func (m *PayloadMetadata) beginMembers() {
	m.pendingRequest, m.pendingResponse = nil, nil
	if m.Field.Type.Name == "base.BaseResp" {
		m.pendingResponse = new(base.BaseResp)
	} else {
		m.pendingRequest = new(base.Base)
	}
}

func (m *PayloadMetadata) recordMember(id int32, value interface{}) error {
	var err error
	if b := m.pendingRequest; b != nil {
		switch id {
		case 1:
			b.LogID, err = metadataString(value)
		case 2:
			b.Caller, err = metadataString(value)
		case 3:
			b.Addr, err = metadataString(value)
		case 6:
			b.Extra, err = metadataExtra(value, false)
		}
	} else if b := m.pendingResponse; b != nil {
		switch id {
		case 1:
			b.StatusMessage, err = metadataString(value)
		case 2:
			switch v := value.(type) {
			case int32:
				b.StatusCode = v
			case int64:
				b.StatusCode = int32(v)
			case int:
				b.StatusCode = int32(v)
			default:
				return fmt.Errorf("invalid wire status %T", value)
			}
		case 3:
			b.Extra, err = metadataExtra(value, false)
		}
	}
	return err
}

func (m *PayloadMetadata) Observe(owner *descriptor.TypeDescriptor, field *descriptor.FieldDescriptor, value interface{}) error {
	if owner != m.Field.Owner || int16(field.ID) != m.Field.FieldID {
		return nil
	}
	object, ok := value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("metadata is not a decoded struct")
	}
	m.Object = object
	m.Request, m.Response = m.pendingRequest, m.pendingResponse
	return nil
}

// PreparePayloadMetadata reads only the metadata node that the writer will
// serialize. A prepared writer consumes this cache instead of parsing Base again.
func PreparePayloadMetadata(value interface{}, field PayloadMetadataField, response bool, cached *base.Base) (*PayloadMetadata, error) {
	m := &PayloadMetadata{Field: field}
	if field.Name == "" {
		return m, nil
	}
	var get func(int16) interface{}
	present := false
	switch body := value.(type) {
	case map[string]interface{}:
		raw, exists := body[field.Name]
		present = exists && raw != nil
		if present {
			object, ok := raw.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("generic metadata %s is not an object", field.Name)
			}
			get = func(id int16) interface{} { return object[field.Members[id]] }
		}
	case string:
		node := gjson.Get(body, field.Name)
		present = node.Exists() && node.Type != gjson.Null
		if present {
			if !node.IsObject() {
				return nil, fmt.Errorf("generic metadata %s is not an object", field.Name)
			}
			get = func(id int16) interface{} { return node.Get(field.Members[id]) }
		}
	case nil:
	default:
		return nil, fmt.Errorf("unsupported generic metadata representation %T", value)
	}
	if !present && cached == nil {
		return m, nil
	}
	if get == nil {
		get = func(int16) interface{} { return nil }
	}
	if err := m.values(get, response, cached); err != nil {
		return nil, err
	}
	return m, nil
}

func metadataExtraObject(original interface{}, extra map[string]string) interface{} {
	switch object := original.(type) {
	case map[string]interface{}:
		for k := range object {
			if _, ok := extra[k]; !ok {
				delete(object, k)
			}
		}
		for k, v := range extra {
			object[k] = v
		}
		return object
	case map[interface{}]interface{}:
		for k := range object {
			s, _ := k.(string)
			if _, ok := extra[s]; !ok {
				delete(object, k)
			}
		}
		for k, v := range extra {
			object[k] = v
		}
		return object
	default:
		created := make(map[string]interface{}, len(extra))
		for k, v := range extra {
			created[k] = v
		}
		return created
	}
}

func (m *PayloadMetadata) ApplyRequest() {
	if !m.Received || m.Object == nil || m.Request == nil {
		return
	}
	b := m.Request
	m.Object[m.Field.Members[1]] = b.LogID
	m.Object[m.Field.Members[2]] = b.Caller
	m.Object[m.Field.Members[3]] = b.Addr
	key := m.Field.Members[6]
	if key != "" {
		m.Object[key] = metadataExtraObject(m.Object[key], b.Extra)
	}
}

func (m *PayloadMetadata) ApplyResponse() {
	if !m.Received || m.Object == nil || m.Response == nil {
		return
	}
	key := m.Field.Members[3]
	if key != "" {
		m.Object[key] = metadataExtraObject(m.Object[key], m.Response.Extra)
	}
}

func writePreparedPayloadMetadata(ctx context.Context, out *wire.BufferWriter, owner *descriptor.TypeDescriptor, field *descriptor.FieldDescriptor) (bool, error) {
	m := payloadMetadataFromContext(ctx)
	if m == nil || m.Field.Owner != owner || int16(field.ID) != m.Field.FieldID || (m.Request == nil && m.Response == nil) {
		return false, nil
	}
	if err := out.WriteFieldBegin(wire.STRUCT, int16(field.ID)); err != nil {
		return true, err
	}
	text := func(id int16, value string) error {
		if err := out.WriteFieldBegin(wire.STRING, id); err != nil {
			return err
		}
		return out.WriteString(value)
	}
	var extra map[string]string
	var extraID int16
	if b := m.Request; b != nil {
		if err := text(1, b.LogID); err != nil {
			return true, err
		}
		if err := text(2, b.Caller); err != nil {
			return true, err
		}
		if err := text(3, b.Addr); err != nil {
			return true, err
		}
		extra, extraID = b.Extra, 6
	} else {
		b := m.Response
		if err := text(1, b.StatusMessage); err != nil {
			return true, err
		}
		if err := out.WriteFieldBegin(wire.I32, 2); err != nil {
			return true, err
		}
		if err := out.WriteI32(b.StatusCode); err != nil {
			return true, err
		}
		extra, extraID = b.Extra, 3
	}
	if extra != nil {
		if err := out.WriteFieldBegin(wire.MAP, extraID); err != nil {
			return true, err
		}
		if err := out.WriteMapBegin(wire.STRING, wire.STRING, len(extra)); err != nil {
			return true, err
		}
		for k, v := range extra {
			if err := out.WriteString(k); err != nil {
				return true, err
			}
			if err := out.WriteString(v); err != nil {
				return true, err
			}
		}
	}
	return true, out.WriteFieldStop()
}
