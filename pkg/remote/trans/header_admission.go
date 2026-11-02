package trans

import (
	"encoding/binary"
	"fmt"

	"github.com/cloudwego/kitex/pkg/remote"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

func prepareDeniedEnvelope(message remote.Message, input remote.ByteBuffer) error {
	prefix, err := input.Peek(8)
	if err != nil {
		return err
	}
	if binary.BigEndian.Uint32(prefix[:4])&0xffff0000 != 0x80010000 {
		return fmt.Errorf("early Mesh admission requires Thrift binary")
	}
	n := int(binary.BigEndian.Uint32(prefix[4:]))
	if n < 0 || n > message.PayloadLen()-12 {
		return fmt.Errorf("invalid Thrift envelope length")
	}
	envelope, err := input.Peek(n + 12)
	if err != nil {
		return err
	}
	if invocation, ok := message.RPCInfo().Invocation().(rpcinfo.InvocationSetter); ok {
		invocation.SetMethodName(string(envelope[8 : 8+n]))
		invocation.SetSeqID(int32(binary.BigEndian.Uint32(envelope[8+n:])))
	}
	return nil
}
