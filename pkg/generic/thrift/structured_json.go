package thrift

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/cloudwego/dynamicgo/conv"
	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	"github.com/tidwall/gjson"
)

// Structured JSON keeps field hooks on the streaming reader/writer. Carry the
// selected conversion options with that backend rather than silently reverting
// to the legacy backend's defaults. HTTP and context Base extraction have a
// different representation contract and cannot be combined with field hooks.
func validateStructuredJSONOptions(options *conv.Options) error {
	if options.EnableHttpMapping || options.EnableThriftBase {
		return fmt.Errorf("structured JSON field hooks do not support HTTP mapping or context Base extraction")
	}
	return nil
}

func (m *ReadJSON) structuredOptions(isClient bool) (*conv.Options, error) {
	if !m.dynamicgoEnabled {
		return nil, nil
	}
	options := m.convOpts
	if isClient {
		options = m.convOptsWithException
	}
	if err := validateStructuredJSONOptions(&options); err != nil {
		return nil, err
	}
	return &options, nil
}

func (m *WriteJSON) structuredOptions(requestBase *Base) (*conv.Options, error) {
	if !m.dynamicgoEnabled {
		return nil, nil
	}
	options := m.convOpts
	if err := validateStructuredJSONOptions(&options); err != nil {
		return nil, err
	}
	if requestBase != nil && options.MergeBaseFunc != nil && reflect.ValueOf(options.MergeBaseFunc).Pointer() != reflect.ValueOf(MergeBase).Pointer() {
		return nil, fmt.Errorf("structured JSON field hooks do not support a custom Base merge function")
	}
	return &options, nil
}

func writeMissingJSONField(field *descriptor.FieldDescriptor, options *conv.Options) (bool, error) {
	if options == nil {
		return !field.Optional, nil
	}
	if field.Required {
		if !options.WriteRequireField {
			return false, fmt.Errorf("required field (%d/%s) missing", field.ID, field.FieldName())
		}
		return true, nil
	}
	if field.Optional {
		return options.WriteOptionalField || field.DefaultValue != nil, nil
	}
	return options.WriteDefaultField, nil
}

// DynamicGo writes descriptor defaults as JSON values, without applying scalar
// conversion options or value mappings a second time to synthesized fields.
func missingJSONValue(field *descriptor.FieldDescriptor) interface{} {
	if field.DefaultValue != nil {
		return field.DefaultValue
	}
	switch field.Type.Type {
	case descriptor.LIST, descriptor.SET:
		return []interface{}{}
	case descriptor.MAP, descriptor.STRUCT:
		return map[string]interface{}{}
	case descriptor.STRING:
		return ""
	default:
		return readEmptyValue(field.Type)
	}
}

func checkJSONScalar(value *gjson.Result, t *descriptor.TypeDescriptor, options *conv.Options) error {
	if options == nil {
		return nil
	}
	numeric := t.Type == descriptor.I08 || t.Type == descriptor.I16 || t.Type == descriptor.I32 || t.Type == descriptor.I64 || t.Type == descriptor.DOUBLE
	if numeric {
		if value.Type != gjson.Number && !(value.Type == gjson.String && options.String2Int64) {
			return fmt.Errorf("expected JSON number for %s", t.Name)
		}
		if t.Type != descriptor.DOUBLE && value.Type == gjson.String {
			text := value.Str
			if _, err := strconv.ParseInt(text, 10, 64); err != nil {
				return err
			}
		}
	}
	if t.Type == descriptor.BOOL && value.Type != gjson.True && value.Type != gjson.False {
		return fmt.Errorf("expected JSON boolean")
	}
	if t.Type == descriptor.STRING && value.Type != gjson.String {
		return fmt.Errorf("expected JSON string")
	}
	if (t.Type == descriptor.LIST || t.Type == descriptor.SET) && !value.IsArray() {
		return fmt.Errorf("expected JSON array")
	}
	if (t.Type == descriptor.MAP || t.Type == descriptor.STRUCT) && !value.IsObject() {
		return fmt.Errorf("expected JSON object")
	}
	return nil
}

// A replacement field still obeys unknown-field validation. Only the replaced
// subtree needs checking here; ordinary fields are checked by their writer.
func checkReplacedJSONFields(value gjson.Result, t *descriptor.TypeDescriptor) error {
	var result error
	switch t.Type {
	case descriptor.STRUCT:
		value.ForEach(func(key, child gjson.Result) bool {
			field, ok := t.Struct.FieldsByName[key.String()]
			if !ok {
				result = fmt.Errorf("unknown field %q", key.String())
				return false
			}
			result = checkReplacedJSONFields(child, field.Type)
			return result == nil
		})
	case descriptor.LIST, descriptor.SET, descriptor.MAP:
		value.ForEach(func(_, child gjson.Result) bool {
			result = checkReplacedJSONFields(child, t.Elem)
			return result == nil
		})
	}
	return result
}
