package clickhouse

import (
	"context"
	"io"
)

// The native protocol server only exchanges Native blocks: producing or
// parsing any other format would have to happen client-side. Client-side
// codecs are deliberately out of scope for the experimental release, so both
// methods return ErrFormatNativeUnsupported. See
// design/support-arbitrary-format.md for the design and the deferred options.

func (c *connect) queryFormat(_ context.Context, release nativeTransportRelease, _ string, _ string, _ ...any) (io.ReadCloser, error) {
	// The connection is healthy and unused - release it back to the pool.
	release(c, nil)
	return nil, ErrFormatNativeUnsupported
}

func (c *connect) insertFormat(_ context.Context, release nativeTransportRelease, _ string, _ string, _ io.Reader) error {
	// The connection is healthy and unused - release it back to the pool.
	release(c, nil)
	return ErrFormatNativeUnsupported
}
