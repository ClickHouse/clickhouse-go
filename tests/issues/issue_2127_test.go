package issues

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

func TestIssue2127(t *testing.T) {
	secure, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(protocol.String(), func(t *testing.T) {
			ctx := t.Context()
			db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, secure, nil)
			require.NoError(t, err)
			t.Cleanup(func() { db.Close() })
			db.SetMaxOpenConns(1)

			table := "issue_2127_" + protocol.String()
			_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" (n UInt64) ENGINE = MergeTree ORDER BY n")
			require.NoError(t, err)
			t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })

			insert := "INSERT INTO " + table + " (n)"
			checkOutsideTx := func() {
				t.Helper()
				stmt, err := db.PrepareContext(ctx, insert)
				if stmt != nil {
					require.NoError(t, stmt.Close())
				}
				require.ErrorContains(t, err, "transaction")
			}
			checkOutsideTx()

			tx, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			stmt, err := tx.PrepareContext(ctx, insert)
			require.NoError(t, err)
			_, err = stmt.ExecContext(ctx, uint64(42))
			require.NoError(t, err)
			require.NoError(t, stmt.Close())
			// Closing the statement leaves the batch pending until commit.
			_, err = tx.PrepareContext(ctx, insert)
			require.ErrorContains(t, err, "pending batch")
			require.NoError(t, tx.Commit())
			checkOutsideTx()

			tx, err = db.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			stmt, err = tx.PrepareContext(ctx, insert)
			require.NoError(t, err)
			_, err = stmt.ExecContext(ctx, uint64(99))
			require.NoError(t, err)
			require.NoError(t, tx.Rollback())
			checkOutsideTx()

			var rows uint64
			require.NoError(t, db.QueryRowContext(ctx, "SELECT count() FROM "+table).Scan(&rows))
			require.Equal(t, uint64(1), rows)
			var n uint64
			require.NoError(t, db.QueryRowContext(ctx, "SELECT n FROM "+table).Scan(&n))
			require.Equal(t, uint64(42), n)
		})
	}
}
