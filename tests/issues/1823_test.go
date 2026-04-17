package issues

import (
	"context"
	"database/sql"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1823 verifies that passing a typed nil pointer to a type implementing
// driver.Valuer with a value receiver (e.g. *uuid.UUID) is bound as NULL
// rather than panicking inside fn.Value().
func Test1823(t *testing.T) {
	const ddl = "CREATE TABLE IF NOT EXISTS test_1823 (id UUID, ref_id Nullable(UUID)) Engine Memory"

	t.Run("native_select", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnectionTCP("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{Method: clickhouse.CompressionLZ4})
		require.NoError(t, err)

		ctx := context.Background()
		var nilUUID *uuid.UUID

		require.NotPanics(t, func() {
			var got *uuid.UUID
			require.NoError(t, conn.QueryRow(ctx, "SELECT ?", nilUUID).Scan(&got))
			require.Nil(t, got)
		})
	})

	t.Run("native_insert", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnectionTCP("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{Method: clickhouse.CompressionLZ4})
		require.NoError(t, err)

		ctx := context.Background()
		require.NoError(t, conn.Exec(ctx, ddl))
		defer conn.Exec(ctx, "DROP TABLE IF EXISTS test_1823")

		id := uuid.New()
		var nilUUID *uuid.UUID

		require.NotPanics(t, func() {
			require.NoError(t, conn.Exec(ctx, "INSERT INTO test_1823 (id, ref_id) VALUES (?, ?)", id, nilUUID))
		})

		var gotRef *uuid.UUID
		require.NoError(t, conn.QueryRow(ctx, "SELECT ref_id FROM test_1823 WHERE id = ?", id).Scan(&gotRef))
		require.Nil(t, gotRef)
	})

	t.Run("std_select", func(t *testing.T) {
		env, err := clickhouse_tests.GetTestEnvironment("issues")
		require.NoError(t, err)
		opts := clickhouse_tests.ClientOptionsFromEnv(env, clickhouse.Settings{}, false)
		db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
		require.NoError(t, err)
		defer db.Close()

		ctx := context.Background()
		var nilUUID *uuid.UUID

		require.NotPanics(t, func() {
			var got uuid.NullUUID
			require.NoError(t, db.QueryRowContext(ctx, "SELECT ?", nilUUID).Scan(&got))
			require.False(t, got.Valid)
		})
	})

	t.Run("std_insert", func(t *testing.T) {
		env, err := clickhouse_tests.GetTestEnvironment("issues")
		require.NoError(t, err)
		opts := clickhouse_tests.ClientOptionsFromEnv(env, clickhouse.Settings{}, false)
		db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
		require.NoError(t, err)
		defer db.Close()

		ctx := context.Background()
		_, err = db.ExecContext(ctx, ddl)
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP TABLE IF EXISTS test_1823")

		id := uuid.New()
		var nilUUID *uuid.UUID

		require.NotPanics(t, func() {
			_, err := db.ExecContext(ctx, "INSERT INTO test_1823 (id, ref_id) VALUES (?, ?)", id, nilUUID)
			require.NoError(t, err)
		})

		var gotRef uuid.NullUUID
		require.NoError(t, db.QueryRowContext(ctx, "SELECT ref_id FROM test_1823 WHERE id = ?", id).Scan(&gotRef))
		require.False(t, gotRef.Valid)
	})
}
