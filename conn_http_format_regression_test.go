package clickhouse

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Finding 1: result data containing the bare "__exception__" token (without
// the server's CRLF framing) must pass through unchanged and NOT be misdetected
// as a mid-stream exception. Any text format can carry the token verbatim and
// any binary format (Parquet/RowBinary/Native/Arrow) can carry it by chance.
func TestExceptionScanReader_BareMarkerPassthrough(t *testing.T) {
	cases := map[string][]byte{
		"text with marker mid-value":  []byte("before__exception__after\n"),
		"text marker then LF only":    []byte("x__exception__\ny"),
		"binary marker (parquet-ish)": append(append([]byte{0x00, 0x01, 0xff}, exceptionMarker...), []byte("PAR1")...),
		"marker at very end no CRLF":  []byte("payload__exception__"),
		"marker then CR only":         []byte("payload__exception__\rmore"),
		"two bare markers":            []byte("a__exception__!b__exception__?c"),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			// Whole read.
			got, err := io.ReadAll(newExceptionScanReader(bytes.NewReader(payload)))
			require.NoError(t, err, "bare marker in data must not fabricate an exception")
			assert.Equal(t, payload, got, "data must pass through unchanged")

			// Byte-by-byte, to exercise the split-boundary holdback path.
			got, err = io.ReadAll(newExceptionScanReader(oneByteReader{bytes.NewReader(payload)}))
			require.NoError(t, err)
			assert.Equal(t, payload, got, "data must pass through unchanged (split reads)")
		})
	}
}

// Finding 2: a compressed InsertFormat whose HTTP request fails before the body
// is read (connection refused) must not strand the compression copy goroutine
// on the caller's reader, and must release the connection.
func TestInsertFormatCompression_NoLeakOnRefusedPort(t *testing.T) {
	pool, err := createCompressionPool(&Compression{Method: CompressionGZIP})
	require.NoError(t, err)
	u, _ := url.Parse("http://127.0.0.1:1/") // port 1 -> connection refused
	h := &httpConnect{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		opt:             &Options{},
		url:             u,
		client:          &http.Client{Timeout: 2 * time.Second},
		compression:     CompressionGZIP,
		compressionPool: pool,
	}

	runtime.GC()
	base := runtime.NumGoroutine()

	// Blocks forever if read. With lazy compression the copy goroutine is only
	// spawned once the HTTP client reads the body, which never happens on a
	// refused connection - so nothing ever touches this reader.
	blocked := make(chan struct{})
	defer close(blocked)
	blocking := readerFunc(func([]byte) (int, error) { <-blocked; return 0, io.EOF })

	released := false
	err = h.insertFormat(context.Background(),
		func(nativeTransport, error) { released = true },
		"JSONEachRow", "INSERT INTO t (a)", blocking)

	require.Error(t, err, "request to a refused port must fail")
	assert.True(t, released, "connection must be released on failure")

	time.Sleep(150 * time.Millisecond)
	runtime.GC()
	leaked := runtime.NumGoroutine() - base
	assert.LessOrEqual(t, leaked, 0, "no goroutine may be stranded on the caller's reader")
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }

// Finding 3: QueryFormat must reject a query carrying its own trailing FORMAT
// clause (the server would honour it over the requested format), without
// false-positiving on the token inside a string literal or identifier.
func TestQueryFormat_TrailingFormatClauseDetection(t *testing.T) {
	reject := []string{
		"SELECT 1 FORMAT JSONEachRow",
		"SELECT 1 format csv",
		"SELECT * FROM t\nFORMAT   Parquet",
		"SELECT 1 SETTINGS max_threads=1 FORMAT CSV",
		"SELECT 1 FORMAT CSV;",
		"SELECT 1 FORMAT CSV  ",
	}
	for _, q := range reject {
		assert.True(t, trailingFormatClause.MatchString(q), "should detect trailing FORMAT: %q", q)
	}

	accept := []string{
		"SELECT 1",
		"SELECT 'FORMAT CSV'",                    // token inside a string literal
		"SELECT * FROM t WHERE c = 'end FORMAT JSON'",
		"SELECT 1 AS myformat",
		"SELECT format FROM t",                   // column named format, not a clause
		"SELECT concat('a','FORMAT CSV') AS x",
	}
	for _, q := range accept {
		assert.False(t, trailingFormatClause.MatchString(q), "must not flag: %q", q)
	}
}
