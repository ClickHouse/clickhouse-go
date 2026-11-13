package issues

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func TestIssue1672_ReadTimeoutSlowConsumer(t *testing.T) {
	const (
		readTimeout = time.Second
		blockSize   = 1000
		blocks      = 40
		blockDelay  = 50 * time.Millisecond
	)

	conn, err := clickhouse_tests.GetConnectionTCPWithOptions("issues", nil, nil, nil, func(o *clickhouse.Options) {
		o.ReadTimeout = readTimeout
	})
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	t.Run("slow consumer", func(t *testing.T) {
		ctx := clickhouse.Context(context.Background(),
			clickhouse.WithBlockBufferSize(1),
			clickhouse.WithSettings(clickhouse.Settings{
				"max_block_size": blockSize,
			}),
		)

		rows, err := conn.Query(ctx, "SELECT number FROM numbers(?)", blockSize*blocks)
		require.NoError(t, err)
		defer rows.Close()

		start := time.Now()
		var count int
		for rows.Next() {
			var n uint64
			require.NoError(t, rows.Scan(&n))
			if n%blockSize == 0 {
				time.Sleep(blockDelay)
			}
			count++
		}
		require.NoError(t, rows.Err())
		require.Equal(t, blockSize*blocks, count)
		require.Greater(t, time.Since(start), readTimeout)
	})

	t.Run("slow server", func(t *testing.T) {
		var v uint8
		err := conn.QueryRow(context.Background(), "SELECT sleep(3)").Scan(&v)
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	})
}
