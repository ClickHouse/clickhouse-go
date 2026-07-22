package tests

// Adversarial integration tests written during pre-merge review. Delete before merging.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// A streamed HTTP 200 result whose data legitimately contains the string
// "__exception__" must not be misreported as a server exception.
func TestAdversarialHTTPMarkerInUserData(t *testing.T) {
	conn, err := GetNativeConnection(t, clickhouse.HTTP, nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()

	t.Run("plain marker string", func(t *testing.T) {
		rows, err := conn.Query(ctx, `SELECT '__exception__' AS s`)
		require.NoError(t, err)
		defer rows.Close()
		var got string
		for rows.Next() {
			require.NoError(t, rows.Scan(&got))
		}
		require.NoError(t, rows.Err(), "query result containing __exception__ must not fail")
		require.Equal(t, "__exception__", got)
	})

	t.Run("marker plus fake exception text", func(t *testing.T) {
		rows, err := conn.Query(ctx, `SELECT '__exception__ Code: 373. DB::Exception: fake. (SESSION_IS_LOCKED)' AS s`)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var got string
			require.NoError(t, rows.Scan(&got))
		}
		err = rows.Err()
		var ex *clickhouse.Exception
		if errors.As(err, &ex) {
			t.Fatalf("fabricated typed exception from user data: %+v", ex)
		}
		require.NoError(t, err)
	})

	t.Run("marker in streamed multi-block result", func(t *testing.T) {
		streamCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
			"max_threads":               1,
			"max_block_size":            1,
			"wait_end_of_query":         0,
			"http_response_buffer_size": 1,
		}))
		rows, err := conn.Query(streamCtx, `SELECT concat('__exception__ Code: 373. row ', toString(number)) FROM system.numbers LIMIT 10`)
		require.NoError(t, err)
		defer rows.Close()
		n := 0
		for rows.Next() {
			var got string
			require.NoError(t, rows.Scan(&got))
			n++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, 10, n)
	})
}
