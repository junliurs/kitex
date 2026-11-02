package remote

import "context"

type UnaryBytesStage uint32

const (
	UnaryBytesEgressRequest UnaryBytesStage = iota + 1
	UnaryBytesIngressRequest
	UnaryBytesIngressResponse
	UnaryBytesEgressResponse
)

type UnaryBytesAction uint32

const (
	UnaryBytesContinue UnaryBytesAction = iota
	UnaryBytesRespond
	UnaryBytesReject
)

type (
	UnaryBytesValueOwner interface{ ReleaseBytesValue(uint64) error }
	UnaryBytesResult     struct {
		Action               UnaryBytesAction
		Lease                uint64
		ValueOwner           UnaryBytesValueOwner
		Bytes                []byte
		ReuseDecodedMetadata bool
	}
)

type UnaryBytesLifecycle interface {
	ProcessBytes(context.Context, *UnaryCall, UnaryBytesStage, []byte) (UnaryBytesResult, error)
}

func ResetUnaryDecodedHeaders(msg Message) {
	if ti, ok := msg.TransInfo().(*transInfo); ok {
		for k := range ti.intInfo {
			delete(ti.intInfo, k)
		}
		for k := range ti.strInfo {
			delete(ti.strInfo, k)
		}
	}
}

// UnaryCaptureLifecycle observes admitted wire bytes. Recording failures never
// replace the RPC result, and the observer must finish borrowing before return.
type UnaryCaptureLifecycle interface {
	CaptureRequested(*UnaryCall) bool
	CaptureFrame(context.Context, *UnaryCall, UnaryBytesStage, []byte)
}

// UnaryCaptureSegmentsLifecycle borrows original transport nodes synchronously.
// It avoids an intermediate contiguous payload solely for sampled recording.
type UnaryCaptureSegmentsLifecycle interface {
	UnaryCaptureLifecycle
	CaptureSegments(context.Context, *UnaryCall, UnaryBytesStage, [][]byte)
}
