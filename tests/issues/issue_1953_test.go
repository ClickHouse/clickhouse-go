package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1953_BindSlashSlashLineComment is a regression test for
// https://github.com/ClickHouse/clickhouse-go/issues/1953: the bind scanner
// recognized `--`, `#`, `#!` and `/* */` comments but not `//`, so `?`, `$N`
// and `@name` inside a `//` comment were substituted as placeholders.
//
// Each query below carries a placeholder inside a `//` comment but supplies
// arguments only for the real placeholders, so before the fix every case
// failed with a "have no arg ..." bind error instead of returning a row.
// The placeholder after the newline proves binding resumes once the comment
// ends. `toInt64(...)` pins the result column type under both protocols; the
// marker inside the comment must stay verbatim text.
func TestIssue1953_BindSlashSlashLineComment(t *testing.T) {
	ctx := context.Background()
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}

	for _, protocol := range protocols {
		t.Run(protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection("issues", t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			t.Run("positional", func(t *testing.T) {
				var a, b int64
				require.NoError(t, conn.QueryRow(ctx, "SELECT toInt64(?) // one ?\n, toInt64(?)", 1, 2).Scan(&a, &b))
				assert.Equal(t, int64(1), a)
				assert.Equal(t, int64(2), b)
			})

			t.Run("numeric", func(t *testing.T) {
				var v int64
				require.NoError(t, conn.QueryRow(ctx, "SELECT toInt64($1) // $2\n", 42).Scan(&v))
				assert.Equal(t, int64(42), v)
			})

			t.Run("named", func(t *testing.T) {
				var v string
				require.NoError(t, conn.QueryRow(ctx, "SELECT @a // @b\n", clickhouse.Named("a", "hello")).Scan(&v))
				assert.Equal(t, "hello", v)
			})
		})
	}
}
