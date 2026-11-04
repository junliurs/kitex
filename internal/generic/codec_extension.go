package generic

import (
	"context"

	"github.com/cloudwego/gopkg/protocol/thrift/base"
	"github.com/cloudwego/kitex/pkg/generic/thrift"
)

func newCodecExtension(ctx context.Context, codec interface{}, method string, response bool) (thrift.MessageCodecExtension, error) {
	if factory := thrift.MessageCodecExtensionFactoryFromContext(ctx); factory != nil {
		return factory.NewMessageCodecExtension(codec, method, response)
	}
	return nil, nil
}

func (g *Args) Codec() interface{}                             { return g.inner }
func (r *Result) Codec() interface{}                           { return r.inner }
func (g *Args) RequestBase() *base.Base                        { return g.base }
func (g *Args) SetRequestBase(value *base.Base)                { g.base = value }
func (g *Args) CodecExtension() thrift.MessageCodecExtension   { return g.codecExtension }
func (r *Result) CodecExtension() thrift.MessageCodecExtension { return r.codecExtension }
func (g *Args) SetCodecExtension(extension thrift.MessageCodecExtension) {
	g.codecExtension = extension
}
func (r *Result) SetCodecExtension(extension thrift.MessageCodecExtension) {
	r.codecExtension = extension
}

func (g *Args) MaterializePayload() (err error) {
	if g.codecExtension != nil {
		g.Request, err = g.codecExtension.Materialize(g.Request)
	}
	return err
}

func (r *Result) MaterializePayload() (err error) {
	if r.codecExtension != nil {
		r.Success, err = r.codecExtension.Materialize(r.Success)
	}
	return err
}
