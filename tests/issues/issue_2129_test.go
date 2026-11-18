package issues

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test2129 verifies that Flush returns the server exception when the batch has
// driver.WithCloseOnFlush and the server rejects the INSERT that Flush closes.
// The batch keeps the rejected rows, so a retry or a later Send sends them again.
// The test table has a `CHECK` constraint that rejects the row with id 1.
// See https://github.com/ClickHouse/clickhouse-go/issues/2129.
func Test2129(t *testing.T) {
	ctx := context.Background()

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%v", protocol), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			t.Run("rejected flush", func(t *testing.T) {
				table := createTable2129(t, conn, fmt.Sprintf("test_2129_rejected_flush_%v", protocol))

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table, driver.WithCloseOnFlush())
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint64(1)))
				if protocol == clickhouse.HTTP {
					// The HTTP batch ignores driver.WithCloseOnFlush, and its Flush does not send the rows.
					require.NoError(t, batch.Flush())
				} else {
					requireViolatedConstraint2129(t, batch.Flush())
				}
				require.Equal(t, 1, batch.Rows(), "the batch must keep the rejected row")
				requireViolatedConstraint2129(t, batch.Send())
				requireRows2129(t, conn, table, 0)
			})

			t.Run("rejected flush after accepted flush", func(t *testing.T) {
				clickhouse_tests.SkipOnHTTP(t, protocol, "the HTTP batch ignores driver.WithCloseOnFlush")
				table := createTable2129(t, conn, fmt.Sprintf("test_2129_accepted_then_rejected_%v", protocol))

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table, driver.WithCloseOnFlush())
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint64(2)))
				require.NoError(t, batch.Flush())
				require.NoError(t, batch.Append(uint64(1)))
				requireViolatedConstraint2129(t, batch.Flush())
				require.NoError(t, batch.Abort())
				// The first Flush ended its own INSERT, so its row stays in the table.
				requireRows2129(t, conn, table, 1)
			})

			t.Run("rejected flush with release connection", func(t *testing.T) {
				clickhouse_tests.SkipOnHTTP(t, protocol, "the HTTP batch ignores driver.WithCloseOnFlush")
				table := createTable2129(t, conn, fmt.Sprintf("test_2129_release_connection_%v", protocol))

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table, driver.WithCloseOnFlush(), driver.WithReleaseConnection())
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint64(1)))
				requireViolatedConstraint2129(t, batch.Flush())
				// A retry sends the same row again on a new connection.
				requireViolatedConstraint2129(t, batch.Flush())
				require.NoError(t, batch.Abort())
				requireRows2129(t, conn, table, 0)
			})

			t.Run("accepted flushes", func(t *testing.T) {
				table := createTable2129(t, conn, fmt.Sprintf("test_2129_accepted_flushes_%v", protocol))

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table, driver.WithCloseOnFlush())
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint64(2)))
				require.NoError(t, batch.Flush())
				require.NoError(t, batch.Append(uint64(3)))
				require.NoError(t, batch.Flush())
				require.NoError(t, batch.Append(uint64(4)))
				require.NoError(t, batch.Send())
				requireRows2129(t, conn, table, 3)
			})

			t.Run("rejected send without close on flush", func(t *testing.T) {
				table := createTable2129(t, conn, fmt.Sprintf("test_2129_without_close_on_flush_%v", protocol))

				batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
				require.NoError(t, err)
				require.NoError(t, batch.Append(uint64(1)))
				// Without driver.WithCloseOnFlush, Flush does not read the server response.
				require.NoError(t, batch.Flush())
				requireViolatedConstraint2129(t, batch.Send())
				requireRows2129(t, conn, table, 0)
			})
		})
	}
}

// createTable2129 creates a table that rejects the row with id 1, and drops it
// when the test ends.
func createTable2129(t *testing.T, conn driver.Conn, table string) string {
	t.Helper()

	require.NoError(t, conn.Exec(context.Background(), fmt.Sprintf(`CREATE TABLE %s
	(
		id UInt64,
		CONSTRAINT id_is_not_1 CHECK id != 1
	)
	ENGINE = MergeTree
	ORDER BY tuple()`, table)), "Create table failed")
	t.Cleanup(func() {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			t.Logf("DROP TABLE %s failed with error %v", table, err)
		}
	})
	return table
}

// requireViolatedConstraint2129 fails the test unless err is the server
// exception `VIOLATED_CONSTRAINT`, code 469.
func requireViolatedConstraint2129(t *testing.T, err error) {
	t.Helper()

	var ex *clickhouse.Exception
	require.Truef(t, errors.As(err, &ex), "want *clickhouse.Exception, got %v", err)
	require.Equal(t, int32(469), ex.Code, "exception %v", ex)
}

// requireRows2129 fails the test unless the table has the given number of rows.
func requireRows2129(t *testing.T, conn driver.Conn, table string, rows uint64) {
	t.Helper()

	var count uint64
	require.NoError(t, conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&count))
	require.Equal(t, rows, count)
}
