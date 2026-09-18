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

func Test1951(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	cases := []struct {
		name, query, want string
		args              []any
	}{
		{"positional", "SELECT $$a?b$$, toInt64(?)", "a?b", []any{42}},
		{"numeric", "SELECT $$1$$, toInt64($1)", "1", []any{42}},
		{"named", "SELECT $$a@n$$, toInt64(@n)", "a@n", []any{clickhouse.Named("n", 42)}},
		{"tagged", "SELECT $tag_2$? $1 @n$tag_2$, toInt64(?)", "? $1 @n", []any{42}},
		{"numeric_tag", "SELECT $1$? $2 @n$1$, toInt64($1)", "? $2 @n", []any{42}},
		{"raw", `SELECT $t$a\?b\$t$, toInt64(?)`, `a\?b\`, []any{42}},
		{"mixed_false_positive", "SELECT $$a?b$$, toInt64($1)", "a?b", []any{42}},
	}
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprint(protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var text string
					var number int64
					require.NoError(t, conn.QueryRow(context.Background(), tc.query, tc.args...).Scan(&text, &number))
					require.Equal(t, tc.want, text)
					require.Equal(t, int64(42), number)
				})
			}
		})
		t.Run("std_"+fmt.Sprint(protocol), func(t *testing.T) {
			opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, nil, protocol == clickhouse.HTTP)
			db, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					var text string
					var number int64
					require.NoError(t, db.QueryRowContext(context.Background(), tc.query, tc.args...).Scan(&text, &number))
					require.Equal(t, tc.want, text)
					require.Equal(t, int64(42), number)
				})
			}
		})
	}
}
