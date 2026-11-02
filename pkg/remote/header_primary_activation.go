package remote

import "context"

// Kept for source compatibility. Messages always use their original maps.
func PreparePrimaryHeaders(context.Context, Message, bool) bool { return false }
