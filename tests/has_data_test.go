package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestRowsHasData(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	t.Run("with data", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 100")
		require.NoError(t, err)
		defer rows.Close()

		assert.True(t, rows.HasData(), "HasData() should return true when query returns rows")
	})

	t.Run("empty result", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 0")
		require.NoError(t, err)
		defer rows.Close()

		assert.False(t, rows.HasData(), "HasData() should return false when query returns no rows")
	})

	t.Run("idempotent", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10")
		require.NoError(t, err)
		defer rows.Close()

		assert.True(t, rows.HasData(), "First HasData() call should return true")
		assert.True(t, rows.HasData(), "Second HasData() call should also return true")
	})

	t.Run("then iterate all rows", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 1000")
		require.NoError(t, err)
		defer rows.Close()

		require.True(t, rows.HasData(), "HasData() should return true")

		var count int
		for rows.Next() {
			var n uint64
			require.NoError(t, rows.Scan(&n))
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 1000, count, "All rows should be iterable after HasData()")
	})

	t.Run("at block boundary", func(t *testing.T) {
		// Force a small block size so the query spans multiple blocks, then call
		// HasData() exactly at the block boundary (r.row == r.block.Rows()).
		// This exercises the r.row = 0 reset when HasData() loads the next block.
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"max_block_size": 5,
		}))
		const total = 10
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10")
		require.NoError(t, err)
		defer rows.Close()

		// Consume exactly the first block (5 rows), landing r.row == r.block.Rows().
		for i := 0; i < 5; i++ {
			require.True(t, rows.Next())
			var n uint64
			require.NoError(t, rows.Scan(&n))
		}

		require.True(t, rows.HasData(), "HasData() should return true when a further block exists")

		var count int
		for rows.Next() {
			var n uint64
			require.NoError(t, rows.Scan(&n))
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, total/2, count, "remaining rows after HasData() at block boundary should all be readable")
	})

	t.Run("after partial iteration", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10")
		require.NoError(t, err)
		defer rows.Close()

		// Consume some rows
		for i := 0; i < 5; i++ {
			require.True(t, rows.Next())
			var n uint64
			require.NoError(t, rows.Scan(&n))
		}

		// After partial iteration, HasData should reflect whether remaining rows exist
		// It should not return a false positive
		require.True(t, rows.HasData(), "HasData() should return true")
		var count int
		for rows.Next() {
			var n uint64
			require.NoError(t, rows.Scan(&n))
			count++
		}
		assert.Greater(t, count, 0, "HasData() returned true, so there should be remaining rows")
	})

	t.Run("after close", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10")
		require.NoError(t, err)

		require.NoError(t, rows.Close())
		assert.False(t, rows.HasData(), "HasData() should return false after Close()")
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 1000000")
		if err != nil {
			cancel()
			return
		}
		defer rows.Close()

		// Cancel after Query() to interrupt the background streaming goroutine.
		// HasData() may return true if the init block already arrived; what matters
		// is that it doesn't block and that the cancellation surfaces as an error.
		cancel()
		rows.HasData()

		for rows.Next() {
		}
		assert.Error(t, rows.Err(), "cancellation should surface as an error during iteration")
	})
}
