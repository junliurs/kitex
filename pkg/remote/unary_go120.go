//go:build !go1.21

package remote

import "context"

// New lifecycle adapters require Go 1.21 cancellation hooks. Existing Kitex
// constructors keep their original Go 1.20 build and execution paths.
const UnaryLifecycleSupported = false

func withoutUnaryCancel(context.Context) context.Context         { panic(ErrUnaryLifecycleProtocol) }
func AfterUnaryCancellation(context.Context, func()) func() bool { panic(ErrUnaryLifecycleProtocol) }
