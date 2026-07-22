package issues

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1877_StructuredHTTPExceptions verifies that server exceptions over
// the HTTP protocol surface as typed errors: errors.As finds *clickhouse.Exception
// (as it always has on native) and *clickhouse.HTTPError carrying the status code
// of the non-200 response. Mid-stream exceptions on a streamed 200 yield a bare
// *clickhouse.Exception with no *clickhouse.HTTPError in the chain — the status
// code was flushed before the query failed and carries no signal.
func TestIssue1877_StructuredHTTPExceptions(t *testing.T) {
	ctx := context.Background()

	const (
		errUnknownTable = 60  // UNKNOWN_TABLE
		errSyntaxError  = 62  // SYNTAX_ERROR
		errThrowIf      = 395 // FUNCTION_THROW_IF_VALUE_IS_NON_ZERO
	)

	// requireException asserts the typed *clickhouse.Exception in the chain.
	// wantCodeName is empty on the native protocol: the wire does not carry
	// the symbolic name and the driver does not fabricate one.
	requireException := func(t *testing.T, err error, wantCode int32, wantCodeName string) *clickhouse.Exception {
		t.Helper()
		require.Error(t, err)
		var ex *clickhouse.Exception
		require.Truef(t, errors.As(err, &ex), "expected *clickhouse.Exception in chain, got: %v", err)
		assert.Equal(t, wantCode, ex.Code)
		assert.Equal(t, "DB::Exception", ex.Name)
		assert.Equal(t, wantCodeName, ex.CodeName)
		return ex
	}

	// Unknown-table query, non-200 response — the core issue scenario. Run
	// without compression, with native compression (LZ4) and with HTTP
	// compression (GZIP): error bodies must parse in all modes.
	for _, compression := range []*clickhouse.Compression{
		nil,
		{Method: clickhouse.CompressionLZ4},
		{Method: clickhouse.CompressionGZIP},
	} {
		name := "query unknown table"
		if compression != nil {
			name += " " + compression.Method.String()
		}
		t.Run(name, func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.HTTP, nil, nil, compression)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			_, err = conn.Query(ctx, "SELECT * FROM issue_1877_no_such_table")
			ex := requireException(t, err, errUnknownTable, "UNKNOWN_TABLE")
			assert.Contains(t, ex.Message, "issue_1877_no_such_table")

			var httpErr *clickhouse.HTTPError
			require.Truef(t, errors.As(err, &httpErr), "expected *clickhouse.HTTPError in chain, got: %v", err)
			assert.GreaterOrEqual(t, httpErr.StatusCode, 400, "status is server-version dependent, but must be an error status")
			assert.Contains(t, err.Error(), "[HTTP ", "error string keeps the [HTTP %d] prefix")
		})
	}

	t.Run("exec syntax error", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.HTTP, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		err = conn.Exec(ctx, "THIS IS NOT VALID SQL")
		requireException(t, err, errSyntaxError, "SYNTAX_ERROR")

		var httpErr *clickhouse.HTTPError
		assert.True(t, errors.As(err, &httpErr))
	})

	t.Run("batch send failure", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.HTTP, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		// The prepared batch holds its pooled connection until Send, so the DDL
		// in between needs a connection of its own.
		connDDL, err := clickhouse_tests.GetConnection("issues", t, clickhouse.HTTP, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { connDDL.Close() })

		// Process-unique name: concurrent CI jobs share one Cloud service.
		table := fmt.Sprintf("issue_1877_batch_%d", os.Getpid())
		require.NoError(t, connDDL.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (n UInt64) ENGINE = MergeTree ORDER BY n", table)))
		t.Cleanup(func() { connDDL.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)) })

		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", table))
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint64(1)))

		// Drop the table between prepare and send so Send fails server-side.
		require.NoError(t, connDDL.Exec(ctx, fmt.Sprintf("DROP TABLE %s", table)))

		err = batch.Send()
		requireException(t, err, errUnknownTable, "UNKNOWN_TABLE")

		var httpErr *clickhouse.HTTPError
		assert.True(t, errors.As(err, &httpErr))
	})

	t.Run("mid-stream exception is bare exception", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.HTTP, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		// Same recipe as TestHTTPExceptionHandling: force the exception to occur
		// after the server has started streaming the 200 response.
		streamCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"max_threads":                           1,
			"max_block_size":                        1,
			"http_write_exception_in_output_format": 0,
			"wait_end_of_query":                     0,
			"http_response_buffer_size":             1,
		}))

		rows, err := conn.Query(streamCtx, `SELECT throwIf(number=3, 'there is an exception') FROM system.numbers`)
		require.NoError(t, err, "query must not fail upfront — the exception arrives mid-stream")

		var streamErr error
		for rows.Next() {
			var result uint8
			if err := rows.Scan(&result); err != nil {
				streamErr = err
				break
			}
		}
		if streamErr == nil {
			streamErr = rows.Err()
		}

		ex := requireException(t, streamErr, errThrowIf, "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO")
		assert.Contains(t, ex.Message, "there is an exception")

		var httpErr *clickhouse.HTTPError
		assert.Falsef(t, errors.As(streamErr, &httpErr),
			"mid-stream failure must not carry *clickhouse.HTTPError — the 200 status was flushed before the query failed")
	})

	t.Run("native protocol control", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnection("issues", t, clickhouse.Native, nil, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		_, err = conn.Query(ctx, "SELECT * FROM issue_1877_no_such_table")
		requireException(t, err, errUnknownTable, "")

		var httpErr *clickhouse.HTTPError
		assert.False(t, errors.As(err, &httpErr), "native protocol must not produce *clickhouse.HTTPError")
	})
}
