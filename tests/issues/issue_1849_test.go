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

// TestDecimalOverflow verifies that appending values to a Decimal(38,0) column
// that exceed the 38-digit precision returns an error containing "overflow"
// instead of silently producing wrong data or panicking.
//
// Regression test for https://github.com/ClickHouse/clickhouse-go/issues/1849.
func TestDecimalOverflow(t *testing.T) {
	const ddl = `CREATE TABLE test_issue_1849 (d128 Decimal(38, 0)) Engine MergeTree() ORDER BY tuple()`

	maxDecimal128, _ := decimal.NewFromString("99999999999999999999999999999999999999")
	justAboveMax, _ := decimal.NewFromString("100000000000000000000000000000000000000")
	minDecimal128, _ := decimal.NewFromString("-99999999999999999999999999999999999999")
	justBelowMin, _ := decimal.NewFromString("-100000000000000000000000000000000000000")

	cases := []struct {
		name  string
		value decimal.Decimal
	}{
		{"positive_overflow_above_max", justAboveMax},
		{"negative_overflow_below_min", justBelowMin},
		{"valid_max_boundary", maxDecimal128},
		{"valid_min_boundary", minDecimal128},
	}

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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
						require.NoError(t, err)
						t.Cleanup(func() { _ = batch.Abort() })

						err = batch.Append(tc.value)
						assertOverflow(t, err, tc.name, "valid_max_boundary", "valid_min_boundary")
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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						err := stdInsertOneDecimal(db, tc.value)
						assertOverflow(t, err, tc.name, "valid_max_boundary", "valid_min_boundary")
					})
				}
			})
		}
	})
}

// TestDecimalSilentDataCorruption verifies that Decimal32 and Decimal64 columns
// no longer silently truncate overflow values via IntPart() casts. The driver
// must return an error containing "overflow" instead of silently producing
// incorrect data.
//
// Regression test for https://github.com/ClickHouse/clickhouse-go/issues/1849.
func TestDecimalSilentDataCorruption(t *testing.T) {
	t.Run("Decimal32", func(t *testing.T) {
		const ddl = `CREATE TABLE test_issue_1849 (d32 Decimal(9, 0)) Engine MergeTree() ORDER BY tuple()`

		maxDecimal32, _ := decimal.NewFromString("999999999")
		justAboveMax32, _ := decimal.NewFromString("1000000000")
		minDecimal32, _ := decimal.NewFromString("-999999999")
		justBelowMin32, _ := decimal.NewFromString("-1000000000")

		cases := []struct {
			name  string
			value decimal.Decimal
		}{
			{"positive_overflow_above_max", justAboveMax32},
			{"negative_overflow_below_min", justBelowMin32},
			{"valid_max_boundary", maxDecimal32},
			{"valid_min_boundary", minDecimal32},
		}

		runDecimalOverflowTest(t, ddl, cases)
	})

	t.Run("Decimal64", func(t *testing.T) {
		const ddl = `CREATE TABLE test_issue_1849 (d64 Decimal(18, 0)) Engine MergeTree() ORDER BY tuple()`

		maxDecimal64, _ := decimal.NewFromString("9999999999999999999")
		justAboveMax64, _ := decimal.NewFromString("10000000000000000000")
		minDecimal64, _ := decimal.NewFromString("-9999999999999999999")
		justBelowMin64, _ := decimal.NewFromString("-10000000000000000000")

		cases := []struct {
			name  string
			value decimal.Decimal
		}{
			{"positive_overflow_above_max", justAboveMax64},
			{"negative_overflow_below_min", justBelowMin64},
			{"valid_max_boundary", maxDecimal64},
			{"valid_min_boundary", minDecimal64},
		}

		runDecimalOverflowTest(t, ddl, cases)
	})
}

// runDecimalOverflowTest runs the given test cases against all 4 surface
// combinations (Native TCP, Native HTTP, Std TCP, Std HTTP) using the provided DDL.
func runDecimalOverflowTest(t *testing.T, ddl string, cases []struct {
	name  string
	value decimal.Decimal
}) {
	t.Helper()

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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
						require.NoError(t, err)
						t.Cleanup(func() { _ = batch.Abort() })

						err = batch.Append(tc.value)
						assertOverflow(t, err, tc.name, "valid_max_boundary", "valid_min_boundary")
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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						err := stdInsertOneDecimal(db, tc.value)
						assertOverflow(t, err, tc.name, "valid_max_boundary", "valid_min_boundary")
					})
				}
			})
		}
	})
}

// TestBigIntOverflow verifies that appending values to Int128 and UInt128
// columns that exceed the type's range returns an error containing "overflow"
// instead of panicking with "math/big: buffer too small".
//
// Regression test for https://github.com/ClickHouse/clickhouse-go/issues/1849.
func TestBigIntOverflow(t *testing.T) {
	t.Run("Int128", func(t *testing.T) {
		const ddl = `CREATE TABLE test_issue_1849 (i128 Int128) Engine MergeTree() ORDER BY tuple()`

		maxInt128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
		minInt128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
		justAboveMaxInt128 := new(big.Int).Add(maxInt128, big.NewInt(1))
		justBelowMinInt128 := new(big.Int).Sub(minInt128, big.NewInt(1))

		cases := []struct {
			name  string
			value *big.Int
		}{
			{"positive_overflow", justAboveMaxInt128},
			{"negative_overflow", justBelowMinInt128},
			{"valid_max_boundary", maxInt128},
			{"valid_min_boundary", minInt128},
		}

		runBigIntOverflowTest(t, ddl, cases, false)
	})

	t.Run("UInt128", func(t *testing.T) {
		const ddl = `CREATE TABLE test_issue_1849 (u128 UInt128) Engine MergeTree() ORDER BY tuple()`

		maxUInt128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))
		justAboveMaxUInt128 := new(big.Int).Lsh(big.NewInt(1), 128)

		cases := []struct {
			name  string
			value *big.Int
		}{
			{"negative_not_allowed", big.NewInt(-1)},
			{"positive_overflow", justAboveMaxUInt128},
			{"valid_max_boundary", maxUInt128},
			{"valid_zero", big.NewInt(0)},
		}

		runBigIntOverflowTest(t, ddl, cases, true)
	})
}

// runBigIntOverflowTest runs the given test cases against all 4 surface
// combinations (Native TCP, Native HTTP, Std TCP, Std HTTP) using the provided DDL.
func runBigIntOverflowTest(t *testing.T, ddl string, cases []struct {
	name  string
	value *big.Int
}, unsigned bool) {
	t.Helper()

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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
						require.NoError(t, err)
						t.Cleanup(func() { _ = batch.Abort() })

						err = batch.Append(tc.value)
						assertBigIntResult(t, err, tc.name, unsigned)
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

				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						err := stdInsertOneBigInt(db, tc.value)
						assertBigIntResult(t, err, tc.name, unsigned)
					})
				}
			})
		}
	})
}

// assertOverflow checks that the error contains "overflow" unless the case
// name matches one of the valid boundary names.
func assertOverflow(t *testing.T, err error, name string, validNames ...string) {
	t.Helper()
	for _, vn := range validNames {
		if name == vn {
			assert.NoError(t, err)
			return
		}
	}
	assert.ErrorContains(t, err, "overflow")
}

// assertBigIntResult checks the expected outcome for BigInt test cases.
func assertBigIntResult(t *testing.T, err error, name string, unsigned bool) {
	t.Helper()
	if unsigned && name == "negative_not_allowed" {
		assert.ErrorContains(t, err, "negative")
		return
	}
	switch name {
	case "valid_max_boundary", "valid_min_boundary", "valid_zero":
		assert.NoError(t, err)
	default:
		assert.ErrorContains(t, err, "overflow")
	}
}

// stdInsertOneDecimal runs a single-row INSERT through the database/sql surface
// for a Decimal column.
func stdInsertOneDecimal(db *sql.DB, value decimal.Decimal) error {
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

	if _, err := stmt.ExecContext(ctx, value); err != nil {
		return err
	}
	return scope.Commit()
}

// stdInsertOneBigInt runs a single-row INSERT through the database/sql surface
// for a BigInt column.
func stdInsertOneBigInt(db *sql.DB, value *big.Int) error {
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

	if _, err := stmt.ExecContext(ctx, value); err != nil {
		return err
	}
	return scope.Commit()
}
