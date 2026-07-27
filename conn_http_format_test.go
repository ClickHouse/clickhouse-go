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
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data), ""))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestExceptionScanReaderDetectsException(t *testing.T) {
	prefix := "1,alice\n2,bob\n"
	src := strings.NewReader(prefix + testExceptionPayload)
	got, err := io.ReadAll(newExceptionScanReader(src, ""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got), "bytes before the marker are served before the error")
}

func TestExceptionScanReaderMarkerAcrossReads(t *testing.T) {
	prefix := "1,alice\n"
	src := oneByteReader{strings.NewReader(prefix + testExceptionPayload)}
	got, err := io.ReadAll(newExceptionScanReader(src, ""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got))
}

func TestExceptionScanReaderPartialMarkerAtEOF(t *testing.T) {
	// A stream legitimately ending in a marker prefix must not lose bytes.
	data := "1,alice\n__excep"
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data), ""))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestExceptionScanReaderUpstreamError(t *testing.T) {
	upstream := errors.New("network broke")
	src := io.MultiReader(strings.NewReader("partial"), errReader{upstream})
	got, err := io.ReadAll(newExceptionScanReader(src, ""))
	require.ErrorIs(t, err, upstream)
	assert.Equal(t, "partial", string(got))
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// testExceptionTag mimics the 16-byte random tag the server generates per
// response and announces in the X-ClickHouse-Exception-Tag header.
const testExceptionTag = "AbCdEfGh12345678"

// taggedExceptionPayload is the framed block a tag-aware server writes on a
// mid-stream failure (WriteBufferFromHTTPServerResponse.cpp).
const taggedExceptionPayload = "__exception__\r\n" + testExceptionTag + "\r\n" +
	"Code: 395. DB::Exception: boom\n31 " + testExceptionTag + "\r\n__exception__\r\n"

func TestExceptionScanReaderTagMismatchIsData(t *testing.T) {
	// Result data that contains the full marker framing but, by construction,
	// cannot contain the per-response tag - must be served verbatim.
	data := "1,\"log line: __exception__\r\nNOT-THE-REAL-TAG1\r\n more text\"\n2,ok\n"
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data), testExceptionTag))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

func TestExceptionScanReaderTagMatchDetects(t *testing.T) {
	prefix := "1,alice\n2,bob\n"
	src := strings.NewReader(prefix + taggedExceptionPayload)
	got, err := io.ReadAll(newExceptionScanReader(src, testExceptionTag))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got))
}

func TestExceptionScanReaderTagSplitAcrossReads(t *testing.T) {
	// Marker and tag arrive one byte at a time: the candidate stays held back
	// until the tag bytes settle it, then the genuine exception surfaces.
	prefix := "1,alice\n"
	src := oneByteReader{strings.NewReader(prefix + taggedExceptionPayload)}
	got, err := io.ReadAll(newExceptionScanReader(src, testExceptionTag))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, prefix, string(got))
}

func TestExceptionScanReaderDataAfterDismissedMarker(t *testing.T) {
	// A genuine exception after data that contains a fake marker: the fake is
	// served as data, the real one (matching tag) still terminates the stream.
	fake := "x,\"__exception__\r\nWRONG-TAG-1234567\r\n\"\n"
	src := strings.NewReader(fake + taggedExceptionPayload)
	got, err := io.ReadAll(newExceptionScanReader(src, testExceptionTag))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, fake, string(got))
}

func TestExceptionScanReaderTruncatedCandidateAtEOF(t *testing.T) {
	// Stream ends inside a marker+partial-tag: a genuine frame cannot be
	// truncated there, so the bytes are data and must not be lost.
	data := "1,alice\n__exception__\r\nAbCdE"
	got, err := io.ReadAll(newExceptionScanReader(strings.NewReader(data), testExceptionTag))
	require.NoError(t, err)
	assert.Equal(t, data, string(got))
}

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
