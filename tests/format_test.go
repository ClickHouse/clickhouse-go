package tests

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const formatTestDDL = `
	CREATE TABLE %s (
		  id         Int64
		, name       String
		, score      Float64
		, ok         Bool
		, created_at DateTime('UTC')
		, comment    Nullable(String)
	) Engine MergeTree() ORDER BY id
`

var formatTestRows = [][]any{
	{int64(1), "alice", 3.5, true, time.Date(2026, 7, 6, 10, 30, 0, 0, time.UTC), "first"},
	{int64(2), "bob", -0.25, false, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), nil},
	{int64(3), "carol, \"quoted\"", 100.0, true, time.Date(2026, 7, 6, 23, 59, 59, 0, time.UTC), `\N looks like null`},
}

func createFormatTestTable(t *testing.T, conn driver.Conn, populate bool) string {
	t.Helper()
	ctx := context.Background()
	table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf(formatTestDDL, table)))
	t.Cleanup(func() {
		conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})
	if populate {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
		require.NoError(t, err)
		for _, row := range formatTestRows {
			require.NoError(t, batch.Append(row...))
		}
		require.NoError(t, batch.Send())
	}
	return table
}

func verifyFormatTestTable(t *testing.T, conn driver.Conn, table string) {
	t.Helper()
	rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT id, name, score, ok, created_at, comment FROM %s ORDER BY id", table))
	require.NoError(t, err)
	defer rows.Close()
	i := 0
	for rows.Next() {
		var (
			id        int64
			name      string
			score     float64
			ok        bool
			createdAt time.Time
			comment   *string
		)
		require.NoError(t, rows.Scan(&id, &name, &score, &ok, &createdAt, &comment))
		want := formatTestRows[i]
		assert.Equal(t, want[0], id)
		assert.Equal(t, want[1], name)
		assert.Equal(t, want[2], score)
		assert.Equal(t, want[3], ok)
		assert.True(t, want[4].(time.Time).Equal(createdAt), "row %d created_at: %v != %v", i, want[4], createdAt)
		if want[5] == nil {
			assert.Nil(t, comment)
		} else {
			require.NotNil(t, comment)
			assert.Equal(t, want[5], *comment)
		}
		i++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, len(formatTestRows), i)
}

// TestFormatRoundTrip streams a table out in each format over HTTP and feeds
// the bytes back into a second table. The server does both conversions.
func TestFormatRoundTrip(t *testing.T) {
	for _, format := range []string{"CSV", "JSONEachRow", "Parquet", "ArrowStream"} {
		t.Run(format, func(t *testing.T) {
			conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
			require.NoError(t, err)
			ctx := context.Background()

			source := createFormatTestTable(t, conn, true)
			dest := createFormatTestTable(t, conn, false)

			stream, err := conn.QueryFormat(ctx, format,
				fmt.Sprintf("SELECT id, name, score, ok, created_at, comment FROM %s ORDER BY id", source))
			require.NoError(t, err)
			payload, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.NoError(t, stream.Close())
			require.NotEmpty(t, payload)

			require.NoError(t, conn.InsertFormat(ctx, format,
				fmt.Sprintf("INSERT INTO %s", dest), bytes.NewReader(payload)))
			verifyFormatTestTable(t, conn, dest)
		})
	}
}

// TestFormatCSVContent pins the exact server-rendered CSV bytes.
func TestFormatCSVContent(t *testing.T) {
	expected := "1,\"alice\",3.5,true,\"2026-07-06 10:30:00\",\"first\"\n" +
		"2,\"bob\",-0.25,false,\"2026-01-01 00:00:00\",\\N\n" +
		"3,\"carol, \"\"quoted\"\"\",100,true,\"2026-07-06 23:59:59\",\"\\N looks like null\"\n"

	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	table := createFormatTestTable(t, conn, true)

	stream, err := conn.QueryFormat(context.Background(), "CSV",
		fmt.Sprintf("SELECT id, name, score, ok, created_at, comment FROM %s ORDER BY id", table))
	require.NoError(t, err)
	defer stream.Close()
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Equal(t, expected, string(payload))
}

// TestFormatInsertStripsFormatClause proves the format argument wins over a
// FORMAT clause embedded in the INSERT statement.
func TestFormatInsertStripsFormatClause(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	table := createFormatTestTable(t, conn, false)

	payload := `{"id":1,"name":"alice","score":3.5,"ok":true,"created_at":"2026-07-06 10:30:00","comment":"first"}
{"id":2,"name":"bob","score":-0.25,"ok":false,"created_at":"2026-01-01 00:00:00","comment":null}
{"id":3,"name":"carol, \"quoted\"","score":100,"ok":true,"created_at":"2026-07-06 23:59:59","comment":"\\N looks like null"}
`
	require.NoError(t, conn.InsertFormat(context.Background(), "JSONEachRow",
		fmt.Sprintf("INSERT INTO %s FORMAT CSV", table), strings.NewReader(payload)))
	verifyFormatTestTable(t, conn, table)
}

// TestFormatNativeProtocolUnsupported verifies the sentinel error over the
// native protocol and that the pool stays healthy after the rejected calls.
func TestFormatNativeProtocolUnsupported(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()

	_, err = conn.QueryFormat(ctx, "CSV", "SELECT 1")
	require.ErrorIs(t, err, clickhouse.ErrFormatNativeUnsupported)

	err = conn.InsertFormat(ctx, "CSV", "INSERT INTO t", strings.NewReader(""))
	require.ErrorIs(t, err, clickhouse.ErrFormatNativeUnsupported)

	require.NoError(t, conn.Exec(ctx, "SELECT 1"))
}

// TestFormatInvalidFormatName verifies format names are validated before
// being interpolated into the query text.
func TestFormatInvalidFormatName(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()

	for _, format := range []string{"", "CSV; DROP TABLE x", "CSV WithNames", "1CSV", "CSV\n"} {
		_, err = conn.QueryFormat(ctx, format, "SELECT 1")
		require.Error(t, err, "format %q must be rejected", format)
		assert.Contains(t, err.Error(), "invalid format name")

		err = conn.InsertFormat(ctx, format, "INSERT INTO t", strings.NewReader(""))
		require.Error(t, err, "format %q must be rejected", format)
		assert.Contains(t, err.Error(), "invalid format name")
	}
}

// TestFormatLargeInsert streams a payload much larger than one HTTP buffer.
func TestFormatLargeInsert(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id Int64, name String) Engine MergeTree() ORDER BY id", table)))
	t.Cleanup(func() { conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

	const rows = 100_000
	var payload strings.Builder
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&payload, "%d,row-%d\n", i, i)
	}
	require.NoError(t, conn.InsertFormat(ctx, "CSV",
		fmt.Sprintf("INSERT INTO %s", table), strings.NewReader(payload.String())))

	var count uint64
	require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&count))
	assert.Equal(t, uint64(rows), count)
}

// TestFormatMalformedInsert verifies the server's parse error surfaces and
// the pool stays usable after the failed insert.
func TestFormatMalformedInsert(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id Int64, name String) Engine MergeTree() ORDER BY id", table)))
	t.Cleanup(func() { conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

	err = conn.InsertFormat(ctx, "CSV",
		fmt.Sprintf("INSERT INTO %s", table), strings.NewReader("1,alice\nnot-a-number,bob\n"))
	require.Error(t, err)

	require.NoError(t, conn.Exec(ctx, "SELECT 1"))

	// A malformed statement (as opposed to a malformed payload) is rejected
	// client-side without consuming a pooled connection.
	err = conn.InsertFormat(ctx, "CSV", "INSERT no-such-syntax", strings.NewReader("1\n"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid INSERT query")

	require.NoError(t, conn.Exec(ctx, "SELECT 1"))
}

// TestFormatMidStreamException forces a server exception after streaming has
// begun; the reader must surface it as an error after any partial data.
func TestFormatMidStreamException(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)

	// http_write_exception_in_output_format is deliberately NOT forced to 0
	// here: queryFormat pins it, and this test proves the pin by using a
	// format (JSON) that would otherwise embed the error inside a valid
	// payload and end the stream cleanly.
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_threads":               1,
		"max_block_size":            1,
		"wait_end_of_query":         0,
		"http_response_buffer_size": 1,
	}))

	stream, err := conn.QueryFormat(ctx, "JSON",
		"SELECT throwIf(number=3, 'there is an exception') FROM system.numbers")
	require.NoError(t, err)
	defer stream.Close()

	_, err = io.ReadAll(stream)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "there is an exception")
}

// TestFormatMidStreamExceptionCompressed proves the exception scan sees the
// marker when the HTTP response is gzip-compressed: servers with tagged
// exception framing write the "__exception__" block through the compression
// buffer, so the decompressed-side scan is the right layer. Older servers
// (<= 25.8) write no block at all on a compressed response - they abort the
// stream - so there the only guarantee is that the read errors (with the
// decompressor's truncation error) instead of silently returning partial
// data.
func TestFormatMidStreamExceptionCompressed(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, clickhouse.Settings{
		"enable_http_compression": 1,
	}, nil, &clickhouse.Compression{Method: clickhouse.CompressionGZIP})
	require.NoError(t, err)
	// Checked before the stream below occupies the pooled connection.
	taggedFraming := CheckMinServerServerVersion(conn, 25, 11, 0)

	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_threads":               1,
		"max_block_size":            1,
		"wait_end_of_query":         0,
		"http_response_buffer_size": 1,
	}))

	// Enough data before the failure that the server flushes compressed
	// chunks and the 200 status is committed before the exception.
	stream, err := conn.QueryFormat(ctx, "JSON",
		"SELECT number, randomPrintableASCII(1000) AS pad, throwIf(number=20000, 'there is an exception') FROM system.numbers")
	require.NoError(t, err)
	defer stream.Close()

	_, err = io.ReadAll(stream)
	require.Error(t, err, "a mid-stream failure must never end the stream cleanly")
	if taggedFraming {
		assert.Contains(t, err.Error(), "there is an exception")
	}
}

// TestFormatCancellation cancels the context mid-read; the reader must
// unblock with an error and the pool must recover.
func TestFormatCancellation(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := conn.QueryFormat(ctx, "CSV", "SELECT number, randomString(100) FROM numbers(100000000)")
	require.NoError(t, err)
	defer stream.Close()

	buf := make([]byte, 4096)
	_, err = stream.Read(buf)
	require.NoError(t, err)
	cancel()

	_, err = io.Copy(io.Discard, stream)
	require.Error(t, err)

	// The stream holds its connection until closed - only then must the pool
	// be usable again.
	require.NoError(t, stream.Close())
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}

// TestFormatMarkerWithFramingInDataStreaming proves tag validation in plain
// streaming mode (no wait_end_of_query): data carrying the full marker
// framing "__exception__\r\n" cannot contain the server's per-response random
// tag, so it must be served verbatim, not misdetected as an exception.
func TestFormatMarkerWithFramingInDataStreaming(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 25, 11, 0) {
		t.Skip("server does not send X-ClickHouse-Exception-Tag; marker alone is trusted best-effort")
	}
	ctx := context.Background()

	table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id Int64, msg String) Engine MergeTree() ORDER BY id", table)))
	t.Cleanup(func() { conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

	require.NoError(t, conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s VALUES (1, concat('__exception__', char(13), char(10), 'FAKE-TAG-12345678', char(13), char(10))), (2, 'after')", table)))

	stream, err := conn.QueryFormat(ctx, "CSV", fmt.Sprintf("SELECT id, msg FROM %s ORDER BY id", table))
	require.NoError(t, err)
	defer stream.Close()
	payload, err := io.ReadAll(stream)
	require.NoError(t, err, "framed marker in data must not be misdetected as an exception")
	assert.Contains(t, string(payload), "__exception__")
	assert.Contains(t, string(payload), "after", "stream must not be truncated at the marker")
}

// TestFormatWaitEndOfQuery covers the all-or-nothing mode: with
// wait_end_of_query=1 the marker scan is bypassed, so data legitimately
// containing "__exception__" survives intact, and a failing query surfaces
// as an error from QueryFormat itself instead of mid-stream.
func TestFormatWaitEndOfQuery(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"wait_end_of_query": 1,
	}))

	t.Run("marker in data is not misdetected", func(t *testing.T) {
		table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
		require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id Int64, msg String) Engine MergeTree() ORDER BY id", table)))
		t.Cleanup(func() { conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

		// A log line that literally contains the in-band exception marker.
		require.NoError(t, conn.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'server wrote __exception__\\r\\n into the log'), (2, 'ok')", table)))

		stream, err := conn.QueryFormat(ctx, "CSV", fmt.Sprintf("SELECT id, msg FROM %s ORDER BY id", table))
		require.NoError(t, err)
		defer stream.Close()
		payload, err := io.ReadAll(stream)
		require.NoError(t, err, "data containing the marker must not be turned into an exception")
		assert.Contains(t, string(payload), "__exception__")
		assert.Contains(t, string(payload), "ok", "stream must not be truncated at the marker")
	})

	t.Run("failure surfaces before any data", func(t *testing.T) {
		stream, err := conn.QueryFormat(ctx, "CSV",
			"SELECT throwIf(number=3, 'there is an exception') FROM system.numbers LIMIT 10")
		if err == nil {
			// Server buffered the result; no partial bytes may arrive.
			payload, readErr := io.ReadAll(stream)
			stream.Close()
			require.Error(t, readErr)
			assert.Empty(t, payload)
			return
		}
		assert.Contains(t, err.Error(), "there is an exception")
	})
}

// TestFormatWaitEndOfQueryConnectionLevel pins that wait_end_of_query set via
// connection-level Options.Settings — not only per-query WithSettings —
// bypasses the marker scan. On pre-tag servers (<= 25.8) data carrying the
// full marker framing survives only through that bypass, so this test guards
// the plumbing on the oldest supported servers too.
func TestFormatWaitEndOfQueryConnectionLevel(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, clickhouse.Settings{
		"wait_end_of_query": 1,
	}, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()

	table := fmt.Sprintf("test_format_%s", RandAsciiString(8))
	require.NoError(t, conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id Int64, msg String) Engine MergeTree() ORDER BY id", table)))
	t.Cleanup(func() { conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

	require.NoError(t, conn.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s VALUES (1, concat('__exception__', char(13), char(10), 'FAKE-TAG-12345678', char(13), char(10))), (2, 'after')", table)))

	stream, err := conn.QueryFormat(ctx, "CSV", fmt.Sprintf("SELECT id, msg FROM %s ORDER BY id", table))
	require.NoError(t, err)
	defer stream.Close()
	payload, err := io.ReadAll(stream)
	require.NoError(t, err, "connection-level wait_end_of_query must bypass the marker scan")
	assert.Contains(t, string(payload), "__exception__")
	assert.Contains(t, string(payload), "after", "stream must not be truncated at the marker")
}

// TestFormatCompression exercises the gzip transport path in both directions,
// with the server actually compressing the response.
func TestFormatCompression(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, clickhouse.Settings{
		"enable_http_compression": 1, // server compresses only when enabled
	}, nil, &clickhouse.Compression{Method: clickhouse.CompressionGZIP})
	require.NoError(t, err)
	ctx := context.Background()

	source := createFormatTestTable(t, conn, true)
	dest := createFormatTestTable(t, conn, false)

	stream, err := conn.QueryFormat(ctx, "JSONEachRow",
		fmt.Sprintf("SELECT id, name, score, ok, created_at, comment FROM %s ORDER BY id", source))
	require.NoError(t, err)
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.NoError(t, stream.Close())

	require.NoError(t, conn.InsertFormat(ctx, "JSONEachRow",
		fmt.Sprintf("INSERT INTO %s", dest), bytes.NewReader(payload)))
	verifyFormatTestTable(t, conn, dest)
}

// TestFormatCompressionServerUncompressed pins the pass-through path: gzip is
// configured on the connection but the server responds uncompressed (no
// Content-Encoding), so the body must be served untouched.
func TestFormatCompressionServerUncompressed(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, clickhouse.Settings{
		"enable_http_compression": 0,
	}, nil, &clickhouse.Compression{Method: clickhouse.CompressionGZIP})
	require.NoError(t, err)

	table := createFormatTestTable(t, conn, true)
	stream, err := conn.QueryFormat(context.Background(), "CSV",
		fmt.Sprintf("SELECT id, name FROM %s ORDER BY id", table))
	require.NoError(t, err)
	defer stream.Close()
	payload, err := io.ReadAll(stream)
	require.NoError(t, err)
	assert.Contains(t, string(payload), "alice")
}
