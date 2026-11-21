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

type issue1955Row interface {
	Scan(...any) error
}

func test1955Named(t *testing.T, name string, queryRow func(context.Context, string, ...any) issue1955Row, idArg, dollarArg any) {
	t.Helper()
	var value, idValue int64
	query := "SELECT toInt64(@" + name + "), toInt64(@id)"
	require.NoError(t, queryRow(context.Background(), query, idArg, dollarArg).Scan(&value, &idValue))
	require.Equal(t, int64(42), value)
	require.Equal(t, int64(7), idValue)
	err := queryRow(context.Background(), query, idArg).Scan(&value, &idValue)
	require.ErrorContains(t, err, fmt.Sprintf("have no arg for %q param", "@"+name))
}

func Test1955(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		t.Run("clickhouse", func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			queryRow := func(ctx context.Context, query string, args ...any) issue1955Row {
				return conn.QueryRow(ctx, query, args...)
			}
			for _, name := range []string{"id$x", "$x"} {
				t.Run(name, func(t *testing.T) {
					test1955Named(t, name, queryRow, clickhouse.Named("id", 7), clickhouse.Named(name, 42))
				})
			}
		})
		t.Run("database_sql", func(t *testing.T) {
			opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, protocol == clickhouse.HTTP)
			db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			queryRow := func(ctx context.Context, query string, args ...any) issue1955Row {
				return db.QueryRowContext(ctx, query, args...)
			}
			for _, name := range []string{"id$x", "$x"} {
				t.Run(name, func(t *testing.T) {
					test1955Named(t, name, queryRow, clickhouse.Named("id", 7), clickhouse.Named(name, 42))
				})
			}
			// database/sql rejects a sql.Named name that does not start with a letter.
			t.Run("sql_named_id$x", func(t *testing.T) {
				test1955Named(t, "id$x", queryRow, sql.Named("id", 7), sql.Named("id$x", 42))
			})
		})
	})
}
