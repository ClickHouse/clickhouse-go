package issues

import (
	"context"
	"database/sql"

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

// TestDecimalOverflow verifies that values outside a Decimal128 storage width
// return an error at the native and database/sql batch APIs.
func TestDecimalOverflow(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		protocol := protocol
		t.Run("native/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			runDecimal128Overflow(t, func(value decimal.Decimal) error {
				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_issue_1849_decimal128")
				if err != nil {
					return err
				}
				defer batch.Abort()
				return batch.Append(value)
			}, func() error {
				ctx := context.Background()
				if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1849_decimal128"); err != nil {
					return err
				}
				return conn.Exec(ctx, "CREATE TABLE test_issue_1849_decimal128 (value Decimal(38, 0)) Engine MergeTree() ORDER BY tuple()")
			})
		})
		t.Run("std/"+protocol.String(), func(t *testing.T) {
			db := issue1849OpenDB(t, protocol)
			defer db.Close()
			runDecimal128Overflow(t, func(value decimal.Decimal) error {
				return issue1849InsertDecimal(db, "test_issue_1849_decimal128", value)
			}, func() error {
				if _, err := db.Exec("DROP TABLE IF EXISTS test_issue_1849_decimal128"); err != nil {
					return err
				}
				_, err := db.Exec("CREATE TABLE test_issue_1849_decimal128 (value Decimal(38, 0)) Engine MergeTree() ORDER BY tuple()")
				return err
			})
		})
	}

}

func runDecimal128Overflow(t *testing.T, appendValue func(decimal.Decimal) error, createTable func() error) {
	t.Helper()
	require.NoError(t, createTable())

	max := decimal.NewFromBigInt(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1)), 0)
	min := decimal.NewFromBigInt(new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127)), 0)
	aboveMax := decimal.NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), 127), 0)
	belowMin := decimal.NewFromBigInt(new(big.Int).Sub(min.Coefficient(), big.NewInt(1)), 0)

	assert.NoError(t, appendValue(max))
	assert.NoError(t, appendValue(min))
	assert.ErrorContains(t, appendValue(aboveMax), "value "+aboveMax.String()+" overflows Decimal128")
	assert.ErrorContains(t, appendValue(belowMin), "value "+belowMin.String()+" overflows Decimal128")
}

// TestDecimalSilentDataCorruption verifies that Decimal32 and Decimal64 no
// longer truncate scaled coefficients that exceed their storage widths.
func TestDecimalSilentDataCorruption(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		protocol := protocol
		t.Run("native/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			runDecimalWidthOverflow(t, "Decimal32", func(value decimal.Decimal) error {
				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_issue_1849_decimal32")
				if err != nil {
					return err
				}
				defer batch.Abort()
				return batch.Append(value)
			}, func() error { return recreateNativeDecimalTable(conn, "test_issue_1849_decimal32", "Decimal(9, 0)") }, 31)
			runDecimalWidthOverflow(t, "Decimal64", func(value decimal.Decimal) error {
				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO test_issue_1849_decimal64")
				if err != nil {
					return err
				}
				defer batch.Abort()
				return batch.Append(value)
			}, func() error { return recreateNativeDecimalTable(conn, "test_issue_1849_decimal64", "Decimal(18, 0)") }, 63)
		})
		t.Run("std/"+protocol.String(), func(t *testing.T) {
			db := issue1849OpenDB(t, protocol)
			defer db.Close()
			runDecimalWidthOverflow(t, "Decimal32", func(value decimal.Decimal) error {
				return issue1849InsertDecimal(db, "test_issue_1849_decimal32", value)
			}, func() error { return recreateStdDecimalTable(db, "test_issue_1849_decimal32", "Decimal(9, 0)") }, 31)
			runDecimalWidthOverflow(t, "Decimal64", func(value decimal.Decimal) error {
				return issue1849InsertDecimal(db, "test_issue_1849_decimal64", value)
			}, func() error { return recreateStdDecimalTable(db, "test_issue_1849_decimal64", "Decimal(18, 0)") }, 63)
		})
	}
}

func runDecimalWidthOverflow(t *testing.T, columnType string, appendValue func(decimal.Decimal) error, createTable func() error, bits uint) {
	t.Helper()
	require.NoError(t, createTable())
	max := decimal.NewFromBigInt(new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), bits), big.NewInt(1)), 0)
	min := decimal.NewFromBigInt(new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), bits)), 0)
	aboveMax := decimal.NewFromBigInt(new(big.Int).Lsh(big.NewInt(1), bits), 0)
	belowMin := decimal.NewFromBigInt(new(big.Int).Sub(min.Coefficient(), big.NewInt(1)), 0)
	assert.NoError(t, appendValue(max))
	assert.NoError(t, appendValue(min))
	assert.ErrorContains(t, appendValue(aboveMax), "value "+aboveMax.String()+" overflows "+columnType)
	assert.ErrorContains(t, appendValue(belowMin), "value "+belowMin.String()+" overflows "+columnType)
}

// TestBigIntOverflow verifies signed and unsigned 128/256-bit overflow checks.
func TestBigIntOverflow(t *testing.T) {
	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		protocol := protocol
		t.Run("native/"+protocol.String(), func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })
			runBigIntCases(t, func(table string, value *big.Int) error {
				batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+table)
				if err != nil {
					return err
				}
				defer batch.Abort()
				return batch.Append(value)
			}, func(table, typ string) error { return recreateNativeDecimalTable(conn, table, typ) })
		})
		t.Run("std/"+protocol.String(), func(t *testing.T) {
			db := issue1849OpenDB(t, protocol)
			defer db.Close()
			runBigIntCases(t, func(table string, value *big.Int) error { return issue1849InsertBigInt(db, table, value) }, func(table, typ string) error { return recreateStdDecimalTable(db, table, typ) })
		})
	}
}

func runBigIntCases(t *testing.T, appendValue func(string, *big.Int) error, createTable func(string, string) error) {
	t.Helper()
	for _, tc := range []struct {
		table, typ string
		bits       uint
		signed     bool
	}{
		{"test_issue_1849_int128", "Int128", 128, true},
		{"test_issue_1849_int256", "Int256", 256, true},
		{"test_issue_1849_uint128", "UInt128", 128, false},
		{"test_issue_1849_uint256", "UInt256", 256, false},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			require.NoError(t, createTable(tc.table, tc.typ))
			limit := new(big.Int).Lsh(big.NewInt(1), tc.bits)
			if tc.signed {
				max := new(big.Int).Sub(new(big.Int).Rsh(new(big.Int).Set(limit), 1), big.NewInt(1))
				min := new(big.Int).Neg(new(big.Int).Rsh(new(big.Int).Set(limit), 1))
				assert.NoError(t, appendValue(tc.table, max))
				assert.NoError(t, appendValue(tc.table, min))
				assert.ErrorContains(t, appendValue(tc.table, new(big.Int).Add(max, big.NewInt(1))), "overflows "+tc.typ)
				assert.ErrorContains(t, appendValue(tc.table, new(big.Int).Sub(min, big.NewInt(1))), "overflows "+tc.typ)
				return
			}
			max := new(big.Int).Sub(new(big.Int).Set(limit), big.NewInt(1))
			assert.NoError(t, appendValue(tc.table, max))
			assert.ErrorContains(t, appendValue(tc.table, limit), "overflows "+tc.typ)
			assert.ErrorContains(t, appendValue(tc.table, big.NewInt(-1)), "negative value -1")
		})
	}
}

func recreateNativeDecimalTable(conn clickhouse.Conn, table, typ string) error {
	ctx := context.Background()
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		return err
	}
	return conn.Exec(ctx, "CREATE TABLE "+table+" (value "+typ+") Engine MergeTree() ORDER BY tuple()")
}

func recreateStdDecimalTable(db *sql.DB, table, typ string) error {
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		return err
	}
	_, err := db.Exec("CREATE TABLE " + table + " (value " + typ + ") Engine MergeTree() ORDER BY tuple()")
	return err
}

func issue1849OpenDB(t *testing.T, protocol clickhouse.Protocol) *sql.DB {
	t.Helper()
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func issue1849InsertDecimal(db *sql.DB, table string, value decimal.Decimal) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO " + table)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(value)
	return err
}

func issue1849InsertBigInt(db *sql.DB, table string, value *big.Int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare("INSERT INTO " + table)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(value)
	return err
}
