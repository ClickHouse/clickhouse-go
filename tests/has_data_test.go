package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		assert.True(t, rh.HasData(), "HasData() should return true when query returns rows")
	})

	t.Run("empty result", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 0")
		require.NoError(t, err)
		defer rows.Close()

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		assert.False(t, rh.HasData(), "HasData() should return false when query returns no rows")
	})

	t.Run("idempotent", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10")
		require.NoError(t, err)
		defer rows.Close()

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		assert.True(t, rh.HasData(), "First HasData() call should return true")
		assert.True(t, rh.HasData(), "Second HasData() call should also return true")
	})

	t.Run("then iterate all rows", func(t *testing.T) {
		ctx := context.Background()
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 1000")
		require.NoError(t, err)
		defer rows.Close()

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		require.True(t, rh.HasData(), "HasData() should return true")

		var count int
		for rows.Next() {
			var n uint64
			require.NoError(t, rows.Scan(&n))
			count++
		}
		require.NoError(t, rows.Err())
		assert.Equal(t, 1000, count, "All rows should be iterable after HasData()")
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

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		// After partial iteration, HasData should reflect whether remaining rows exist
		// It should not return a false positive
		require.True(t, rh.HasData(), "HasData() should return true")
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

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")

		rows.Close()
		assert.False(t, rh.HasData(), "HasData() should return false after Close()")
	})

	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 1000000")
		if err != nil {
			return
		}
		defer rows.Close()

		rh, ok := rows.(driver.RowsWithHasData)
		require.True(t, ok, "rows should implement RowsWithHasData")
		assert.False(t, rh.HasData(), "HasData() should return false due to cancelled context")
		assert.Error(t, rows.Err(), "Err() should be non-nil when HasData returns false due to cancellation")
	})
}
