package issues

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1916_HTTPCompressedEOFDoesNotLog verifies that a normal end of an
// LZ4-compressed HTTP response is not reported as an exception-block drain
// failure. The compressed reader wraps io.EOF, which is the production path
// that previously produced the spurious error log.
func TestIssue1916_HTTPCompressedEOFDoesNotLog(t *testing.T) {
	env, err := clickhouse_tests.GetIssuesTestEnvironment()
	require.NoError(t, err)

	var logBuf bytes.Buffer
	opts := clickhouse_tests.ClientOptionsFromEnv(env, nil, true)
	opts.Logger = slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}

	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 1")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	var count int
	for rows.Next() {
		var number uint64
		require.NoError(t, rows.Scan(&number))
		count++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 1, count)
	require.NotContains(t, logBuf.String(), "level=ERROR")
}
