//go:build go1.21

package remote

import "context"

// UnaryLifecycleSupported gates the opt-in lifecycle. Legacy Kitex remains Go 1.20.
const UnaryLifecycleSupported = true

func withoutUnaryCancel(ctx context.Context) context.Context { return context.WithoutCancel(ctx) }

// AfterUnaryCancellation preserves the callback path without watcher goroutines.
func AfterUnaryCancellation(ctx context.Context, callback func()) func() bool {
	return context.AfterFunc(ctx, callback)
}
