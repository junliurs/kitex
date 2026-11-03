package generic

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cloudwego/gopkg/bufiox"

	"github.com/cloudwego/kitex/pkg/generic/descriptor"
	genericthrift "github.com/cloudwego/kitex/pkg/generic/thrift"
)

type (
	payloadReadCost   struct{ NextCalls, NextBytes, PeekCalls, PeekBytes, CopyCalls, CopyBytes, SkipCalls, SkipBytes int }
	payloadCostReader struct {
		bufiox.Reader
		cost payloadReadCost
	}
)

func (r *payloadCostReader) Next(n int) ([]byte, error) {
	r.cost.NextCalls++
	r.cost.NextBytes += n
	return r.Reader.Next(n)
}

func (r *payloadCostReader) Peek(n int) ([]byte, error) {
	r.cost.PeekCalls++
	r.cost.PeekBytes += n
	return r.Reader.Peek(n)
}

func (r *payloadCostReader) ReadBinary(b []byte) (int, error) {
	r.cost.CopyCalls++
	n, e := r.Reader.ReadBinary(b)
	r.cost.CopyBytes += n
	return n, e
}

func (r *payloadCostReader) Skip(n int) error {
	r.cost.SkipCalls++
	r.cost.SkipBytes += n
	return r.Reader.Skip(n)
}

func payloadCostInput(size int) (*descriptor.ServiceDescriptor, []byte) {
	svc := metadataDescriptor()
	req := svc.Functions["Visit"].Request.Struct.FieldsByID[7].Type.Struct
	f := &descriptor.FieldDescriptor{ID: 1, Name: "Body", Type: &descriptor.TypeDescriptor{Type: descriptor.STRING, Name: "string"}}
	req.FieldsByID[1] = f
	req.FieldsByName["Body"] = f
	body := []byte(strings.Repeat("x", size))
	prefix := []byte{12, 0, 7, 11, 0, 1, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(prefix[6:], uint32(size))
	raw := append(prefix, body...)
	raw = append(raw, requestMetadataBytes(true)[3:]...)
	return svc, raw
}

func payloadCostCodec(svc *descriptor.ServiceDescriptor, jsonMode bool) interface{} {
	if jsonMode {
		return genericthrift.NewJsonReaderWriter(svc)
	}
	return genericthrift.NewStructReaderWriter(svc)
}

func payloadCostRead(ctx context.Context, codec interface{}, raw []byte, patch bool) (*Args, payloadReadCost, error) {
	a := &Args{}
	a.SetCodec(codec)
	reader := &payloadCostReader{Reader: bufiox.NewBytesReader(raw)}
	defer reader.Release(nil)
	if err := a.Read(ctx, "Visit", 0, reader); err != nil {
		return nil, reader.cost, err
	}
	if genericthrift.PayloadMetadataEnabled(ctx) {
		_, base, received, err := a.RequestBaseMetadata("Visit")
		if err != nil || !received || base == nil {
			return nil, reader.cost, fmt.Errorf("metadata: %v %v", received, err)
		}
		if patch {
			base.Caller = "governed"
			a.SetRequestBaseMetadata(base)
		}
		if err := a.MaterializePayload(); err != nil {
			return nil, reader.cost, err
		}
	}
	if reader.ReadLen() != len(raw) {
		return nil, reader.cost, fmt.Errorf("boundary: %d/%d", reader.ReadLen(), len(raw))
	}
	return a, reader.cost, nil
}

// Compare the real streaming codec with and without governance observations.
// Reader counters exclude the codec's normal allocations/string ownership.
func TestUnaryPayloadCostSingleTraversal(t *testing.T) {
	for _, size := range []int{64, 131072} {
		for _, jsonMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("json=%v/body=%d", jsonMode, size), func(t *testing.T) {
				svc, raw := payloadCostInput(size)
				codec := payloadCostCodec(svc, jsonMode)
				_, plain, err := payloadCostRead(context.Background(), codec, raw, false)
				if err != nil {
					t.Fatal(err)
				}
				ctx := genericthrift.WithPayloadMetadataEnabled(context.Background())
				for _, patch := range []bool{false, true} {
					a, observed, err := payloadCostRead(ctx, codec, raw, patch)
					if err != nil {
						t.Fatal(err)
					}
					if plain != observed {
						t.Fatalf("extra wire traversal: plain=%+v observed=%+v", plain, observed)
					}
					if n := testing.AllocsPerRun(100, func() { _, _, _, _ = a.RequestBaseMetadata("Visit") }); n != 0 {
						t.Fatalf("cached lookup allocated: %v", n)
					}
					if !jsonMode {
						object := a.Request.(map[string]interface{})
						before := reflect.ValueOf(object).Pointer()
						_, base, _, _ := a.RequestBaseMetadata("Visit")
						base.LogID = "patched-log"
						a.SetRequestBaseMetadata(base)
						if before != reflect.ValueOf(a.Request).Pointer() || object["Body"].(string) != strings.Repeat("x", size) {
							t.Fatal("patch replaced or changed business body")
						}
					}
					t.Logf("patch=%v wire=%d counters=%+v cached_lookup_allocs=0", patch, len(raw), observed)
				}
			})
		}
	}
}

// A local codec diagnostic, not network performance acceptance or Native cost.
func BenchmarkUnaryPayloadCodecCost(b *testing.B) {
	for _, size := range []int{64, 131072} {
		for _, jsonMode := range []bool{false, true} {
			for _, mode := range []string{"codec", "metadata", "patch"} {
				b.Run(fmt.Sprintf("json=%v/body=%d/%s", jsonMode, size, mode), func(b *testing.B) {
					svc, raw := payloadCostInput(size)
					codec := payloadCostCodec(svc, jsonMode)
					ctx := context.Background()
					if mode != "codec" {
						ctx = genericthrift.WithPayloadMetadataEnabled(ctx)
					}
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, _, err := payloadCostRead(ctx, codec, raw, mode == "patch"); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
