package issues

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1955(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			for _, name := range []string{"id$x", "$x", "id$", "$", "id$1"} {
				t.Run(name, func(t *testing.T) {
					var value, prefix int64
					query := "SELECT toInt64(@" + name + "), toInt64(@id)"
					require.NoError(t, conn.QueryRow(context.Background(), query,
						clickhouse.Named("id", 7), clickhouse.Named(name, 42)).Scan(&value, &prefix))
					require.Equal(t, int64(42), value)
					require.Equal(t, int64(7), prefix)
					err := conn.QueryRow(context.Background(), query, clickhouse.Named("id", 7)).Scan(&value, &prefix)
					require.ErrorContains(t, err, "@"+name)
				})
			}
		})
		t.Run("std_"+fmt.Sprint(protocol), func(t *testing.T) {
			opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, protocol == clickhouse.HTTP)
			db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			for _, name := range []string{"id$x", "$x", "id$", "$", "id$1"} {
				t.Run(name, func(t *testing.T) {
					var value, prefix int64
					query := "SELECT toInt64(@" + name + "), toInt64(@id)"
					require.NoError(t, db.QueryRowContext(context.Background(), query,
						clickhouse.Named("id", 7), clickhouse.Named(name, 42)).Scan(&value, &prefix))
					require.Equal(t, int64(42), value)
					require.Equal(t, int64(7), prefix)
					err := db.QueryRowContext(context.Background(), query, clickhouse.Named("id", 7)).Scan(&value, &prefix)
					require.ErrorContains(t, err, "@"+name)
				})
			}
		})
	}
}
