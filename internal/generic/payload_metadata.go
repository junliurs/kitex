package generic

import (
	"fmt"

	"github.com/cloudwego/gopkg/protocol/thrift/base"

	"github.com/cloudwego/kitex/pkg/generic/thrift"
)

type payloadMetadataCodec interface {
	PayloadMetadataField(string, bool) (thrift.PayloadMetadataField, bool, error)
}

func lookupPayloadMetadata(inner interface{}, method string, response bool) (thrift.PayloadMetadataField, bool, error) {
	codec, ok := inner.(payloadMetadataCodec)
	if !ok {
		return thrift.PayloadMetadataField{}, false, fmt.Errorf("generic codec has no structured payload metadata")
	}
	return codec.PayloadMetadataField(method, response)
}

func (g *Args) RequestBaseMetadata(method string) (thrift.PayloadMetadataField, *base.Base, bool, error) {
	if g.payloadMetadata != nil {
		return g.payloadMetadata.Field, g.payloadMetadata.Request, g.payloadMetadata.Received, nil
	}
	field, _, err := lookupPayloadMetadata(g.inner, method, false)
	if err != nil {
		return field, nil, false, err
	}
	metadata, err := thrift.PreparePayloadMetadata(g.Request, field, false, g.base)
	if err != nil {
		return field, nil, false, err
	}
	g.payloadMetadata = metadata
	g.base = metadata.Request
	return field, g.base, false, nil
}

func (g *Args) SetRequestBaseMetadata(value *base.Base) {
	g.base = value
	if g.payloadMetadata != nil {
		g.payloadMetadata.Request = value
		g.payloadMetadata.ApplyRequest()
	}
}

func (r *Result) ResponseBaseMetadata(method string) (thrift.PayloadMetadataField, *base.BaseResp, bool, error) {
	if r.payloadMetadata != nil {
		return r.payloadMetadata.Field, r.payloadMetadata.Response, r.payloadMetadata.Received, nil
	}
	field, _, err := lookupPayloadMetadata(r.inner, method, true)
	if err != nil {
		return field, nil, false, err
	}
	metadata, err := thrift.PreparePayloadMetadata(r.Success, field, true, nil)
	if err != nil {
		return field, nil, false, err
	}
	r.payloadMetadata = metadata
	return field, metadata.Response, false, nil
}

func (r *Result) SetResponseBaseMetadata(value *base.BaseResp) {
	if r.payloadMetadata != nil {
		r.payloadMetadata.Response = value
		r.payloadMetadata.ApplyResponse()
	}
}

func (g *Args) MaterializePayload() error {
	if g.payloadMetadata != nil && g.payloadMetadata.JSON != nil {
		value, err := g.payloadMetadata.JSON.Materialize()
		if err != nil {
			return err
		}
		g.Request = value
		g.payloadMetadata.JSON = nil
	}
	return nil
}

func (r *Result) MaterializePayload() error {
	if r.payloadMetadata != nil && r.payloadMetadata.JSON != nil {
		value, err := r.payloadMetadata.JSON.Materialize()
		if err != nil {
			return err
		}
		r.Success = value
		r.payloadMetadata.JSON = nil
	}
	return nil
}
