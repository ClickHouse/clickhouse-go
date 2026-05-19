package issues

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strconv"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// overflow1849Cases captures the four overflow scenarios that previously
// either panicked (BigInt) or silently truncated (Decimal). For each case the
// driver must now return an error containing "overflow".
func overflow1849Cases(t *testing.T) []struct {
	name string
	d128 any
	i128 any
} {
	t.Helper()
	big2_127 := new(big.Int).Lsh(big.NewInt(1), 127)
	min128 := new(big.Int).Neg(big2_127)
	belowMin128 := new(big.Int).Sub(min128, big.NewInt(1))

	return []struct {
		name string
		d128 any
		i128 any
	}{
		{"Decimal128PositiveOverflow", decimal.NewFromBigInt(big2_127, 0), big.NewInt(0)},
		{"Decimal128NegativeOverflow", decimal.NewFromBigInt(belowMin128, 0), big.NewInt(0)},
		{"Int128PositiveOverflow", decimal.NewFromBigInt(big.NewInt(0), 0), *big2_127},
		{"Int128NegativeOverflow", decimal.NewFromBigInt(big.NewInt(0), 0), *belowMin128},
	}
}

// TestIssue1849 verifies that appending overflow values to Decimal and
// BigInt columns returns an error instead of panicking. Covered surfaces:
//   - native driver.Conn over TCP
//   - native driver.Conn over HTTP
//   - database/sql over TCP
//   - database/sql over HTTP
//
// Regression test for https://github.com/ClickHouse/clickhouse-go/issues/1849.
func TestIssue1849(t *testing.T) {
	const ddl = `CREATE TABLE test_issue_1849 (
		d128 Decimal(38, 0),
		i128 Int128
	) Engine MergeTree() ORDER BY tuple()`

	t.Run("Native", func(t *testing.T) {
		ctx := context.Background()
		for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
			t.Run(protocol.String(), func(t *testing.T) {
				conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
				require.NoError(t, err)
				t.Cleanup(func() { conn.Close() })

				require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1849"))
				require.NoError(t, conn.Exec(ctx, ddl))
				t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1849") })

				for _, tc := range overflow1849Cases(t) {
					t.Run(tc.name, func(t *testing.T) {
						batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
						require.NoError(t, err)
						t.Cleanup(func() { _ = batch.Abort() })

						err = batch.Append(tc.d128, tc.i128)
						assert.ErrorContains(t, err, "overflow")
					})
				}
			})
		}
	})

	t.Run("Std", func(t *testing.T) {
		useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)

		for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
			t.Run(protocol.String(), func(t *testing.T) {
				db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
				require.NoError(t, err)
				t.Cleanup(func() { db.Close() })

				_, _ = db.Exec("DROP TABLE IF EXISTS test_issue_1849")
				_, err = db.Exec(ddl)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS test_issue_1849") })

				for _, tc := range overflow1849Cases(t) {
					t.Run(tc.name, func(t *testing.T) {
						appendErr := stdInsertOverflow(db, tc.d128, tc.i128)
						assert.ErrorContains(t, appendErr, "overflow")
					})
				}
			})
		}
	})
}

// stdInsertOverflow runs a single-row INSERT through the database/sql
// surface and returns the first error encountered. The overflow check
// fires inside the column converter, so the error normally surfaces from
// ExecContext. We still drive the full Begin → Prepare → Exec → Commit
// flow so any caller-visible regression is caught.
func stdInsertOverflow(db *sql.DB, d128, i128 any) error {
	ctx := context.Background()
	scope, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = scope.Rollback() }()

	stmt, err := scope.PrepareContext(ctx, "INSERT INTO test_issue_1849")
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	if _, err := stmt.ExecContext(ctx, d128, i128); err != nil {
		return err
	}
	return scope.Commit()
}
