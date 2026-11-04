package transmeta

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/bytedance/gopkg/cloud/metainfo"

	"github.com/cloudwego/kitex/pkg/kerrors"
	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/remote/transmeta"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/pkg/serviceinfo"
	"github.com/cloudwego/kitex/transport"
)

func headerMetadataMessage(t *testing.T, response bool) remote.Message {
	t.Helper()
	cfg := rpcinfo.NewRPCConfig()
	mutable := rpcinfo.AsMutableRPCConfig(cfg)
	mutable.SetTransportProtocol(transport.TTHeaderFramed)
	mutable.SetPayloadCodec(serviceinfo.Thrift)
	mutable.SetRPCTimeout(3 * time.Second)
	mutable.SetConnectTimeout(time.Second)
	from := rpcinfo.NewEndpointInfo("initial-from", "initial-method", nil, nil)
	to := rpcinfo.NewEndpointInfo("destination", "Echo", nil, nil)
	invocation := rpcinfo.NewInvocation("IDL", "Echo")
	ri := rpcinfo.NewRPCInfo(from, to, invocation, cfg, rpcinfo.NewRPCStats())
	typ := remote.Call
	if response {
		typ = remote.Reply
	}
	msg := remote.NewMessage(nil, ri, typ, remote.Client)
	t.Cleanup(func() { remote.RecycleMessage(msg) })

	return msg
}

func headerMetadataHeaders(info remote.TransInfo) (map[uint16]string, map[string]string) {
	ints, strings := make(map[uint16]string), make(map[string]string)
	h := remote.AccessHeaders(info)
	h.RangeInt(func(k uint16, v string) bool { ints[k] = v; return true })
	h.RangeString(func(k, v string) bool { strings[k] = v; return true })
	return ints, strings
}

func TestHeaderClientTTHeaderMetadata(t *testing.T) {
	for _, locked := range []bool{false, true} {
		msg := headerMetadataMessage(t, false)
		h := remote.AccessHeaders(msg.TransInfo())
		h.SetInt(65535, "unknown")
		h.SetString("preserved", "value")
		if locked {
			cfg := rpcinfo.AsMutableRPCConfig(msg.RPCInfo().Config())
			cfg.LockConfig(rpcinfo.BitRPCTimeout)
			cfg.LockConfig(rpcinfo.BitConnectTimeout)
		}
		if _, err := ClientTTHeaderHandler.WriteMeta(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
		ints, strings := headerMetadataHeaders(msg.TransInfo())
		if ints[transmeta.TransportType] != framedTransportType || strings[transmeta.HeaderIDLServiceName] != "IDL" || ints[65535] != "unknown" || strings["preserved"] != "value" {
			t.Fatalf("lost metadata: %v %v", ints, strings)
		}
		_, hasTimeout := ints[transmeta.RPCTimeout]
		_, hasConnectTimeout := ints[transmeta.ConnectTimeout]
		if hasTimeout != locked || hasConnectTimeout != locked {
			t.Fatalf("locked=%v timeouts=%v", locked, ints)
		}
	}
}

func TestHeaderServerTTHeaderRead(t *testing.T) {
	for _, timeout := range []string{"97", "", "invalid"} {
		msg := headerMetadataMessage(t, false)
		h := remote.AccessHeaders(msg.TransInfo())
		h.MergeInt(map[uint16]string{transmeta.FromService: "caller", transmeta.FromMethod: "Call", transmeta.RPCTimeout: timeout})
		if _, err := ServerTTHeaderHandler.ReadMeta(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
		wantTimeout := 3 * time.Second
		if timeout == "97" {
			wantTimeout = 97 * time.Millisecond
		}
		if msg.RPCInfo().From().ServiceName() != "caller" || msg.RPCInfo().From().Method() != "Call" || msg.RPCInfo().Config().RPCTimeout() != wantTimeout {
			t.Fatalf("timeout=%q RPCInfo=%v", timeout, msg.RPCInfo())
		}
	}
}

func TestHeaderResponseTTHeaderMetadata(t *testing.T) {
	msg := headerMetadataMessage(t, true)
	msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter).SetBizStatusErr(kerrors.NewBizStatusErrorWithExtra(17, "denied", map[string]string{"reason": "policy"}))
	rpcinfo.AsTaggable(msg.RPCInfo().To()).SetTag(rpcinfo.ConnResetTag, "")
	if _, err := ServerTTHeaderHandler.WriteMeta(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if _, err := ClientTTHeaderHandler.ReadMeta(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	h := remote.AccessHeaders(msg.TransInfo())
	if val, ok := h.StringValue(transmeta.HeaderConnectionReadyToReset); !ok || val != "" {
		t.Fatal("empty reset header presence was lost")
	}
	if h.String(bizStatus) != "17" || h.String(bizMessage) != "denied" || h.String(bizExtra) != `{"reason":"policy"}` {
		t.Fatal("business status was lost")
	}
	for _, extra := range []string{`{"key":"value"}`, "{invalid", ""} {
		input := map[string]string{bizStatus: "17", bizMessage: "failure", bizExtra: extra}
		h.MergeString(input)
		want, wantErr := ParseBizStatusErr(input)
		got, gotErr := parseBizStatusErr(h.String)
		if (wantErr == nil) != (gotErr == nil) || !reflect.DeepEqual(got, want) {
			t.Fatalf("public map helper differs for extra=%q: %v/%v %v/%v", extra, got, gotErr, want, wantErr)
		}
	}
}

func TestHeaderMetainfoBackwardAllFields(t *testing.T) {
	fields := map[string]string{"ordinary": "visible", "": "empty-key", "empty-value": "", metainfo.PrefixTransient + "meta": "value"}
	msg := headerMetadataMessage(t, true)
	remote.AccessHeaders(msg.TransInfo()).MergeString(fields)
	ctx := metainfo.WithBackwardValues(context.Background())
	metainfo.SetBackwardValue(ctx, "retained", "old")
	metainfo.SetBackwardValue(ctx, "ordinary", "overwrite")
	ctx, err := MetainfoClientHandler.ReadMeta(ctx, msg)
	if err != nil {
		t.Fatal(err)
	}
	got := metainfo.RecvAllBackwardValues(ctx)
	if got["retained"] != "old" || len(got) != len(fields)+1 {
		t.Fatalf("backward merge lost fields: %v", got)
	}
	for key, want := range fields {
		if value, ok := got[key]; !ok || value != want {
			t.Fatalf("backward key %q: %q/%v want %q", key, value, ok, want)
		}
	}
}

func TestHeaderEmptyMapAlias(t *testing.T) {
	msg := headerMetadataMessage(t, true)
	ints, strings := make(map[uint16]string), make(map[string]string)
	msg.TransInfo().PutTransIntInfo(ints)
	msg.TransInfo().PutTransStrInfo(strings)

	msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter).SetBizStatusErr(kerrors.NewBizStatusError(17, "denied"))
	if _, err := ServerTTHeaderHandler.WriteMeta(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if ints[transmeta.MsgType] == "" || strings[bizStatus] != "17" || strings[bizMessage] != "denied" {
		t.Fatalf("direct writes detached old empty maps: %v %v", ints, strings)
	}
	msg.TransInfo().PutTransStrInfo(map[string]string{"merged": "value"})
	if strings["merged"] != "value" {
		t.Fatal("Put detached a nonempty map")
	}

	client := headerMetadataMessage(t, false)
	emptyInt, emptyString := make(map[uint16]string), make(map[string]string)
	client.TransInfo().PutTransIntInfo(emptyInt)
	client.TransInfo().PutTransStrInfo(emptyString)
	if _, err := ClientTTHeaderHandler.WriteMeta(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	if len(emptyInt) != 0 || emptyString[transmeta.HeaderIDLServiceName] != "IDL" {
		t.Fatalf("legacy client Put/direct-write distinction changed: %v %v", emptyInt, emptyString)
	}
	replacement := map[string]string{"replacement": "first"}
	other := headerMetadataMessage(t, false)
	other.TransInfo().PutTransStrInfo(map[string]string{})
	other.TransInfo().PutTransStrInfo(replacement)
	replacement["replacement"] = "second"
	if remote.AccessHeaders(other.TransInfo()).String("replacement") != "second" {
		t.Fatal("Put failed to preserve replacement map alias")
	}
}

type headerMetadataPutProbe struct {
	remote.TransInfo
	intPuts, strPuts int
}

func (p *headerMetadataPutProbe) PutTransIntInfo(values map[uint16]string) {
	p.intPuts++
	p.TransInfo.PutTransIntInfo(values)
}

func (p *headerMetadataPutProbe) PutTransStrInfo(values map[string]string) {
	p.strPuts++
	p.TransInfo.PutTransStrInfo(values)
}

type headerMetadataCustomMessage struct {
	remote.Message
	info remote.TransInfo
}

func (m headerMetadataCustomMessage) TransInfo() remote.TransInfo { return m.info }

func TestHeaderCustomTransInfoDirectWrites(t *testing.T) {
	msg := headerMetadataMessage(t, true)
	ints, strings := msg.TransInfo().TransIntInfo(), msg.TransInfo().TransStrInfo()
	probe := &headerMetadataPutProbe{TransInfo: msg.TransInfo()}
	custom := headerMetadataCustomMessage{Message: msg, info: probe}
	msg.RPCInfo().Invocation().(rpcinfo.InvocationSetter).SetBizStatusErr(kerrors.NewBizStatusError(17, "denied"))
	if _, err := ServerTTHeaderHandler.WriteMeta(context.Background(), custom); err != nil {
		t.Fatal(err)
	}
	if probe.intPuts != 0 || probe.strPuts != 0 {
		t.Fatalf("legacy direct writes invoked custom Put: int=%d str=%d", probe.intPuts, probe.strPuts)
	}
	if ints[transmeta.MsgType] == "" || strings[bizStatus] != "17" || strings[bizMessage] != "denied" {
		t.Fatalf("legacy custom maps lost writes: %v %v", ints, strings)
	}
}
