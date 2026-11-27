package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

func TestIssue1854(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		settings := clickhouse.Settings{
			"allow_experimental_json_type":    1,
			"allow_experimental_dynamic_type": 1,
			"allow_experimental_variant_type": 1,
		}
		opts := clickhouse_tests.ClientOptionsFromEnv(env, settings, protocol == clickhouse.HTTP)
		opts.MaxOpenConns, opts.MaxIdleConns = 1, 1
		t.Run("ClickHouse", func(t *testing.T) {
			conn, err := clickhouse.Open(&opts)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			if !clickhouse_tests.CheckMinServerServerVersion(conn, 24, 8, 0) {
				t.Skip("JSON requires ClickHouse 24.8")
			}
			checkIssue1854(t, clickhouse_tests.CheckMinServerServerVersion(conn, 25, 6, 0), func(ctx context.Context, q string, dest ...any) error {
				return conn.QueryRow(ctx, q).Scan(dest...)
			})
		})
		t.Run("database_sql", func(t *testing.T) {
			db := clickhouse.OpenDB(&opts)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			if !clickhouse_std_tests.CheckMinServerVersion(db, 24, 8, 0) {
				t.Skip("JSON requires ClickHouse 24.8")
			}
			checkIssue1854(t, clickhouse_std_tests.CheckMinServerVersion(db, 25, 6, 0), func(ctx context.Context, q string, dest ...any) error {
				return db.QueryRowContext(ctx, q).Scan(dest...)
			})
		})
	})
}

func checkIssue1854(t *testing.T, supportsFlattened bool, scan func(context.Context, string, ...any) error) {
	t.Helper()
	const setting = "output_format_native_use_flattened_dynamic_and_json_serialization"
	legacy := context.Background()
	if supportsFlattened {
		legacy = clickhouse.Context(legacy, clickhouse.WithSettings(clickhouse.Settings{setting: 0}))
	}
	for _, tc := range []struct{ name, expr, storage string }{
		{"JSON", `'{"x":13,"y":"east"}'::JSON(max_dynamic_paths=0)`, "JSON shared data"},
		{"JSON_Dynamic", `'{"x":13,"y":"east"}'::JSON(max_dynamic_types=0)`, "Dynamic SharedVariant"},
		{"Dynamic", `toInt64(13)::Dynamic(max_types=0)`, "Dynamic SharedVariant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := "SELECT " + tc.expr + ", 'tail'"
			var doc chcol.JSON
			var value chcol.Dynamic
			var dest any = &doc
			if tc.name == "Dynamic" {
				dest = &value
			}
			var tail string
			err := scan(legacy, query, dest, &tail)
			require.ErrorContains(t, err, "cannot decode "+tc.storage)
			require.ErrorContains(t, err, setting+"=1")
			var one uint8
			require.NoError(t, scan(context.Background(), "SELECT toUInt8(1)", &one))
			require.Equal(t, uint8(1), one)
			if !supportsFlattened {
				return
			}
			flattened := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{setting: 1}))
			require.NoError(t, scan(flattened, query, dest, &tail))
			require.Equal(t, "tail", tail)
			if tc.name == "Dynamic" {
				require.Equal(t, int64(13), value.Any())
				return
			}
			x, ok := chcol.ExtractJSONPathAs[int64](&doc, "x")
			require.True(t, ok)
			require.Equal(t, int64(13), x)
			y, ok := chcol.ExtractJSONPathAs[string](&doc, "y")
			require.True(t, ok)
			require.Equal(t, "east", y)
		})
	}
}
