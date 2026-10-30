package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestStructIterProtocols(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)

		ctx := context.Background()
		const table = "test_struct_iter"
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
		t.Cleanup(func() {
			require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS "+table))
		})
		require.NoError(t, conn.Exec(ctx, `
			CREATE TABLE test_struct_iter (
				Col1 UInt8,
				Col2 String
			) ENGINE = Memory
		`))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_struct_iter")
		require.NoError(t, err)
		require.NoError(t, batch.Append(uint8(1), "one"))
		require.NoError(t, batch.Append(uint8(2), "two"))
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT Col1, Col2 FROM test_struct_iter ORDER BY Col1")
		require.NoError(t, err)

		type result struct {
			Col1 uint8
			Col2 string
		}
		var got []result
		for value, err := range driver.StructIter[result](rows) {
			require.NoError(t, err)
			got = append(got, value)
		}

		require.Equal(t, []result{
			{Col1: 1, Col2: "one"},
			{Col1: 2, Col2: "two"},
		}, got)
	})
}

func TestStructIterLifecycle(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
			"max_block_size": 1000,
			"max_threads":    1,
		}, nil, nil)
		require.NoError(t, err)

		t.Run("early break", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			rows, err := conn.Query(ctx, "SELECT number AS Value FROM numbers(100000)")
			require.NoError(t, err)
			type item struct{ Value uint64 }
			count := 0
			for value, err := range driver.StructIter[item](rows) {
				require.NoError(t, err)
				require.Zero(t, value.Value)
				count++
				break
			}
			require.Equal(t, 1, count)
			require.False(t, rows.HasData(), "iterator must close rows on early break")
			require.Eventually(t, func() bool { return conn.Stats().Open == 0 },
				5*time.Second, 10*time.Millisecond, "iterator must release its connection")
			require.NoError(t, conn.Ping(ctx))
		})

		t.Run("mid-stream error", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// Large blocks flush HTTP response headers before the server exception.
			rows, err := conn.Query(ctx, `SELECT number AS Value, randomPrintableASCII(1000) AS Pad,
    throwIf(number=20000, 'struct iter mid-stream failure') AS Failure
    FROM numbers(30000)`)
			require.NoError(t, err)
			type item struct {
				Value   uint64
				Pad     string
				Failure uint8
			}
			count, errorCount := 0, 0
			var terminalErr error
			for value, err := range driver.StructIter[item](rows) {
				if err != nil {
					errorCount++
					terminalErr = err
					require.Equal(t, item{}, value)
					continue // Also verify that no further values or errors are yielded.
				}
				require.Zero(t, errorCount)
				count++
			}
			require.Positive(t, count, "failure must occur after successful rows")
			require.Equal(t, 1, errorCount)
			require.ErrorContains(t, terminalErr, "struct iter mid-stream failure")
			require.Same(t, rows.Err(), terminalErr, "terminal error must not be joined with itself")
			require.False(t, rows.HasData())
			require.Eventually(t, func() bool { return conn.Stats().Open == 0 },
				5*time.Second, 10*time.Millisecond)
			require.NoError(t, conn.Ping(ctx))
		})
	})
}
