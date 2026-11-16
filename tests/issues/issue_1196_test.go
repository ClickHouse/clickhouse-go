package issues

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1196 checks that DNS SRV support can be toggled via DSN and Options
// without breaking existing connection semantics.
func Test1196(t *testing.T) {
	t.Run("resolve_srv=false keeps plain host dial", func(t *testing.T) {
		conn, err := clickhouse_tests.GetConnectionTCPWithOptions("issues", nil, nil, nil, func(o *clickhouse.Options) {
			o.ResolveSRV = false
		})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		require.NoError(t, conn.Ping(context.Background()))
	})

	t.Run("ResolveSRV=true against a name with no SRV records fails with resolve SRV error", func(t *testing.T) {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr:       []string{"_clickhouse._tcp.invalid"},
			ResolveSRV: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		err = conn.Ping(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolve SRV")
	})

	t.Run("database/sql: resolve_srv=true against a name with no SRV records fails with resolve SRV error", func(t *testing.T) {
		db, err := sql.Open("clickhouse", "clickhouse://_clickhouse._tcp.invalid?resolve_srv=true")
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })

		err = db.Ping()
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), "resolve SRV"), "expected resolve SRV error, got: %v", err)
	})
}
