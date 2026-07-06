package clickhouse

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// oneByteReader forces marker detection across read boundaries.
type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return o.r.Read(p)
}

const testExceptionPayload = "__exception__\r\nCode: 395. DB::Exception: boom\n42 ABC\r\n__exception__"

func TestExceptionScanReaderCleanStream(t *testing.T) {
	data := strings.Repeat("1,alice\n2,bob\n", 1000)
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data)))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestExceptionScanReaderDetectsException(t *testing.T) {
	prefix := "1,alice\n2,bob\n"
	src := strings.NewReader(prefix + testExceptionPayload)
	got, err := io.ReadAll(newExceptionScanReader(src))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got), "bytes before the marker are served before the error")
}

func TestExceptionScanReaderMarkerAcrossReads(t *testing.T) {
	prefix := "1,alice\n"
	src := oneByteReader{strings.NewReader(prefix + testExceptionPayload)}
	got, err := io.ReadAll(newExceptionScanReader(src))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got))
}

func TestExceptionScanReaderPartialMarkerAtEOF(t *testing.T) {
	// A stream legitimately ending in a marker prefix must not lose bytes.
	data := "1,alice\n__excep"
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data)))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestExceptionScanReaderUpstreamError(t *testing.T) {
	upstream := errors.New("network broke")
	src := io.MultiReader(strings.NewReader("partial"), errReader{upstream})
	got, err := io.ReadAll(newExceptionScanReader(src))
	require.ErrorIs(t, err, upstream)
	assert.Equal(t, "partial", string(got))
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestHTTPFormatStreamCloseIdempotent(t *testing.T) {
	pool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)
	released := 0
	s := &httpFormatStream{
		reader:  bytes.NewReader(nil),
		body:    io.NopCloser(bytes.NewReader(nil)),
		conn:    &httpConnect{compressionPool: pool},
		release: func(nativeTransport, error) { released++ },
	}
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	assert.Equal(t, 1, released, "release must happen exactly once")
	_, err = s.Read(make([]byte, 1))
	require.Error(t, err)
}
