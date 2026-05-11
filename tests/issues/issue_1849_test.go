package issues

import (
	"context"
	"math/big"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// TestIssue1849 verifies that appending overflow values to Decimal and BigInt
// columns returns an error instead of panicking.
func TestIssue1849(t *testing.T) {
	ctx := context.Background()

	conn, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	const ddl = `
		CREATE TABLE test_issue_1849 (
			d128 Decimal(38, 0),
			i128 Int128
		) Engine MergeTree() ORDER BY tuple()
	`
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1849")
	require.NoError(t, conn.Exec(ctx, ddl))
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS test_issue_1849")

	t.Run("Decimal128PositiveOverflow", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
		require.NoError(t, err)

		big2_127 := new(big.Int).Lsh(big.NewInt(1), 127)
		overflow := decimal.NewFromBigInt(big2_127, 0)
		err = batch.AppendRow(overflow, big.NewInt(0))
		assert.ErrorContains(t, err, "overflow")
	})

	t.Run("Decimal128NegativeOverflow", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
		require.NoError(t, err)

		// -2^127 - 1 is below the minimum Decimal128 signed range
		minVal := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
		overflow := decimal.NewFromBigInt(new(big.Int).Sub(minVal, big.NewInt(1)), 0)
		err = batch.AppendRow(overflow, big.NewInt(0))
		assert.ErrorContains(t, err, "overflow")
	})

	t.Run("Int128PositiveOverflow", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
		require.NoError(t, err)

		big2_127 := new(big.Int).Lsh(big.NewInt(1), 127)
		err = batch.AppendRow(decimal.NewFromBigInt(big.NewInt(0), 0), *big2_127)
		assert.ErrorContains(t, err, "overflow")
	})

	t.Run("Int128NegativeOverflow", func(t *testing.T) {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_issue_1849")
		require.NoError(t, err)

		// -2^127 - 1 is below the minimum Int128 value (-2^127)
		minInt128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
		overflow := new(big.Int).Sub(minInt128, big.NewInt(1))
		err = batch.AppendRow(decimal.NewFromBigInt(big.NewInt(0), 0), *overflow)
		assert.ErrorContains(t, err, "overflow")
	})
}
