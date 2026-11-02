package generic

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	genericthrift "github.com/cloudwego/kitex/pkg/generic/thrift"
	"github.com/cloudwego/kitex/pkg/remote"
)

// This intentionally uses only APIs shared with the pre-Buffered revision.
// Both arms decode the same framed body and return the same wire metadata.
func BenchmarkMeshlibExistingCodec(b *testing.B) {
	for _, size := range []int{64, 131072} {
		for _, jsonMode := range []bool{false, true} {
			for _, withHeader := range []bool{false, true} {
				b.Run(fmt.Sprintf("json=%v/body=%d/ttheader=%v", jsonMode, size, withHeader), func(b *testing.B) {
					svc := metadataDescriptor()
					req := svc.Functions["Visit"].Request.Struct.FieldsByID[7].Type.Struct
					f := &descriptor.FieldDescriptor{ID: 1, Name: "Body", Type: &descriptor.TypeDescriptor{Type: descriptor.STRING, Name: "string"}}
					req.FieldsByID[1] = f
					req.FieldsByName["Body"] = f
					prefix := []byte{12, 0, 7, 11, 0, 1, 0, 0, 0, 0}
					binary.BigEndian.PutUint32(prefix[6:], uint32(size))
					raw := append(prefix, strings.Repeat("x", size)...)
					raw = append(raw, requestMetadataBytes(true)[3:]...)
					var codec interface{} = genericthrift.NewStructReaderWriter(svc)
					if jsonMode {
						codec = genericthrift.NewJsonReaderWriter(svc)
					}
					c := remote.NewUnaryCall(nil)
					defer c.Recycle()
					c.RequestWire = remote.UnaryWire{Framed: true, WithTTHeader: withHeader}
					ctx := remote.WithUnaryCall(context.Background(), c)
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						a := &Args{}
						a.SetCodec(codec)
						r := bufiox.NewBytesReader(raw)
						err := a.Read(ctx, "Visit", len(raw), r)
						if err == nil && !withHeader {
							_, base, received, e := a.RequestBaseMetadata("Visit")
							err = e
							if !received || base == nil || base.Caller != "wire-caller" {
								b.Fatal("wrong wire metadata")
							}
						}
						if finalizer, ok := interface{}(a).(interface{ MaterializePayload() error }); ok && err == nil {
							err = finalizer.MaterializePayload()
						}
						if r.ReadLen() != len(raw) {
							b.Fatal("wrong codec boundary")
						}
						_ = r.Release(nil)
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
