package issues

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test944 verifies that Options.QuotaKey sets the quota key for the connection.
// The server counts the queries of a quota KEYED BY client_key per key, so the
// queries of one key exceed the quota and the queries of a different key don't.
// See https://github.com/ClickHouse/clickhouse-go/issues/944.
func Test944(t *testing.T) {
	env, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	ctx := context.Background()

	admin, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close() })

	suffix := clickhouse_tests.RandAsciiString(6)
	table := fmt.Sprintf("`%s`.test_944_%s", env.Database, suffix)
	username := fmt.Sprintf("quota_user_%s", suffix)
	password := clickhouse_tests.RandAsciiString(10) + "1#"
	quota := fmt.Sprintf("quota_944_%s", suffix)
	const maxQueries = 5

	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (id UInt64) ENGINE = MergeTree ORDER BY id", table)))
	t.Cleanup(func() {
		if err := admin.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Logf("DROP TABLE %s failed: %v", table, err)
		}
	})
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(10)", table)))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", username, password)))
	t.Cleanup(func() {
		if err := admin.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %s", username)); err != nil {
			t.Logf("DROP USER %s failed: %v", username, err)
		}
	})
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf("GRANT SELECT ON %s TO %s", table, username)))
	require.NoError(t, admin.Exec(ctx, fmt.Sprintf(
		"CREATE QUOTA %s KEYED BY client_key FOR INTERVAL 1 hour MAX queries = %d TO %s", quota, maxQueries, username)))
	t.Cleanup(func() {
		if err := admin.Exec(ctx, fmt.Sprintf("DROP QUOTA IF EXISTS %s", quota)); err != nil {
			t.Logf("DROP QUOTA %s failed: %v", quota, err)
		}
	})

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			open := func(quotaKey string) clickhouse.Conn {
				opts := clickhouse_tests.ClientOptionsFromEnv(env, nil, protocol == clickhouse.HTTP)
				opts.Auth.Username = username
				opts.Auth.Password = password
				opts.QuotaKey = quotaKey
				conn, err := clickhouse.Open(&opts)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })
				return conn
			}
			count := func(conn clickhouse.Conn) error {
				var n uint64
				return conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&n)
			}

			exhausted := open(fmt.Sprintf("tenant_a_%s", protocol))
			for range maxQueries {
				if err = count(exhausted); err != nil {
					break
				}
			}
			if err == nil {
				err = count(exhausted)
			}
			var exception *clickhouse.Exception
			require.True(t, errors.As(err, &exception), "expected a server exception, got: %v", err)
			require.Equal(t, int32(201), exception.Code, "expected QUOTA_EXCEEDED, got: %v", err)

			require.NoError(t, count(open(fmt.Sprintf("tenant_b_%s", protocol))))
		})
	}
}
