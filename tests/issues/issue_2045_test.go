package issues

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

func TestIssue2045IntegerOverflow(t *testing.T) {
	ctx := context.Background()
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		protocol := protocol
		t.Run("native/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			createIssue2045Table(t, func(query string) error { return conn.Exec(ctx, query) })
			checkIssue2045Overflow(t, func(signed, unsigned any) error {
				batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_2045")
				if err != nil {
					return err
				}
				defer batch.Abort()
				if err := batch.Append(signed, unsigned); err != nil {
					return err
				}
				return batch.Send()
			}, func(signed *int16, unsigned *uint8) error {
				return conn.QueryRow(ctx, "SELECT signed_value, unsigned_value FROM test_issue_2045").Scan(signed, unsigned)
			})
		})

		t.Run("database/sql/"+protocol.String(), func(t *testing.T) {
			db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })

			createIssue2045Table(t, func(query string) error {
				_, err := db.ExecContext(ctx, query)
				return err
			})
			checkIssue2045Overflow(t, func(signed, unsigned any) error {
				_, err := db.ExecContext(ctx, "INSERT INTO test_issue_2045 VALUES (?, ?)", signed, unsigned)
				return err
			}, func(signed *int16, unsigned *uint8) error {
				return db.QueryRowContext(ctx, "SELECT signed_value, unsigned_value FROM test_issue_2045").Scan(signed, unsigned)
			})
		})
	}
}

func createIssue2045Table(t *testing.T, exec func(string) error) {
	t.Helper()
	require.NoError(t, exec("DROP TABLE IF EXISTS test_issue_2045"))
	require.NoError(t, exec("CREATE TABLE test_issue_2045 (signed_value Int16, unsigned_value UInt8) ENGINE = Memory"))
	t.Cleanup(func() { assert.NoError(t, exec("DROP TABLE IF EXISTS test_issue_2045")) })
}

func checkIssue2045Overflow(t *testing.T, insert func(any, any) error, query func(*int16, *uint8) error) {
	t.Helper()
	require.ErrorContains(t, insert(int64(40000), uint8(1)), "overflow")
	require.ErrorContains(t, insert(int64(1), int64(-1)), "overflow")
	require.NoError(t, insert(int64(32767), int16(255)))

	var signed int16
	var unsigned uint8
	require.NoError(t, query(&signed, &unsigned))
	assert.Equal(t, int16(32767), signed)
	assert.Equal(t, uint8(255), unsigned)
}
