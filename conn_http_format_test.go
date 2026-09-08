package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	chproto "github.com/ClickHouse/ch-go/proto"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

func TestExceptionScanReaderTruncatedExceptionBlock(t *testing.T) {
	// The connection breaks while the exception block itself is being
	// drained: the error must say so instead of silently degrading into a
	// truncated message.
	upstream := errors.New("conn reset")
	src := io.MultiReader(
		strings.NewReader("data\n__exception__\r\nCode: 395. DB::Exception: bo"),
		errReader{upstream})
	got, err := io.ReadAll(newExceptionScanReader(src, ""))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exception block truncated")
	assert.Equal(t, "data\n", string(got))
}

func TestWaitEndOfQueryEnabledPrecedence(t *testing.T) {
	// Connection-level Options.Settings set the default; a per-query setting
	// overrides it in either direction.
	connLevel := &httpConnect{waitEndOfQuery: true}
	assert.True(t, connLevel.waitEndOfQueryEnabled(Settings{}))
	assert.False(t, connLevel.waitEndOfQueryEnabled(Settings{"wait_end_of_query": 0}))

	perQuery := &httpConnect{}
	assert.False(t, perQuery.waitEndOfQueryEnabled(Settings{}))
	assert.True(t, perQuery.waitEndOfQueryEnabled(Settings{"wait_end_of_query": 1}))
	assert.True(t, perQuery.waitEndOfQueryEnabled(Settings{"wait_end_of_query": "true"}))
}

func newTestHTTPConnect(t *testing.T, rawURL string) *httpConnect {
	t.Helper()
	pool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)
	u, err := url.Parse(rawURL)
	require.NoError(t, err)
	return &httpConnect{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		opt:             &Options{},
		url:             u,
		client:          &http.Client{Timeout: 5 * time.Second},
		compressionPool: pool,
		buffer:          new(chproto.Buffer),
	}
}

func TestInsertFormatInBandExceptionOn200(t *testing.T) {
	// A failure after the server flushed its 200 status arrives in-band as an
	// "__exception__" block in the response body; insertFormat must report it
	// instead of a silent success.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(taggedExceptionPayload))
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	var relErr error
	err := h.insertFormat(context.Background(),
		func(_ nativeTransport, e error) { relErr = e },
		"CSV", "INSERT INTO t (a)", strings.NewReader("1\n"))
	require.Error(t, err)
	var ex *Exception
	require.ErrorAs(t, err, &ex, "in-band block must parse into a typed exception")
	assert.Equal(t, int32(395), ex.Code)
	require.Error(t, relErr, "connection must be released with the error")
}

func TestInsertFormatBadStatementReleasesHealthy(t *testing.T) {
	// A malformed INSERT is a caller mistake caught client-side: the never-
	// used connection must go back to the pool, not be closed as broken.
	h := newTestHTTPConnect(t, "http://127.0.0.1:1") // never dialed
	released := false
	var relErr error
	err := h.insertFormat(context.Background(),
		func(_ nativeTransport, e error) { released = true; relErr = e },
		"CSV", "INSERT t", strings.NewReader("1\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid INSERT query")
	assert.True(t, released)
	assert.NoError(t, relErr, "client-side parse failure must not poison the connection")
}

func TestInsertFormatEmptyBodyIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	released := false
	var relErr error
	err := h.insertFormat(context.Background(),
		func(_ nativeTransport, e error) { released = true; relErr = e },
		"CSV", "INSERT INTO t (a)", strings.NewReader("1\n"))
	require.NoError(t, err)
	assert.True(t, released)
	assert.NoError(t, relErr)
}

// newTestHTTPBatch prepares a batch against h without the DESCRIBE TABLE
// round-trip, by supplying the column types up front.
func newTestHTTPBatch(t *testing.T, h *httpConnect, release nativeTransportRelease) driver.Batch {
	t.Helper()
	ctx := Context(context.Background(), WithColumnNamesAndTypes([]ColumnNameAndType{
		{Name: "a", Type: "Int64"},
	}))
	b, err := h.prepareBatch(ctx, release, nil, "INSERT INTO t (a)", driver.PrepareBatchOptions{})
	require.NoError(t, err)
	return b
}

func TestBatchSendInBandExceptionOn200(t *testing.T) {
	// Same hole as insertFormat: a batch INSERT whose failure arrives after
	// the 200 headers must fail Send, not report success.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(taggedExceptionPayload))
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	var relErr error
	batch := newTestHTTPBatch(t, h, func(_ nativeTransport, e error) { relErr = e })
	require.NoError(t, batch.Append(int64(1)))

	err := batch.Send()
	require.Error(t, err)
	var ex *Exception
	require.ErrorAs(t, err, &ex, "in-band block must parse into a typed exception")
	assert.Equal(t, int32(395), ex.Code)
	require.Error(t, relErr, "connection must be released with the error")
}

func TestBatchSendEmptyBodyIsSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := newTestHTTPConnect(t, srv.URL)
	released := false
	var relErr error
	batch := newTestHTTPBatch(t, h, func(_ nativeTransport, e error) { released = true; relErr = e })
	require.NoError(t, batch.Append(int64(1)))

	require.NoError(t, batch.Send())
	assert.True(t, released)
	assert.NoError(t, relErr)
}

// endlessReader serves an unbounded stream and counts what was consumed.
type endlessReader struct{ n int64 }

func (r *endlessReader) Read(p []byte) (int, error) {
	r.n += int64(len(p))
	return len(p), nil
}

func TestHTTPFormatStreamCloseBoundedDrain(t *testing.T) {
	// A caller abandoning a large result must not block in Close while the
	// remainder transfers: the drain is bounded, past it the connection is
	// forfeited instead.
	pool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)
	body := &endlessReader{}
	released := false
	s := &httpFormatStream{
		reader:  body,
		body:    io.NopCloser(body),
		conn:    &httpConnect{compressionPool: pool},
		release: func(nativeTransport, error) { released = true },
	}
	require.NoError(t, s.Close())
	assert.True(t, released)
	assert.LessOrEqual(t, body.n, int64(maxCloseDrainBytes),
		"Close must not drain more than the bound from an abandoned stream")
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
