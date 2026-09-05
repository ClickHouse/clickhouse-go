package column

import (
	"math/big"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecimal32OverflowReturnsError(t *testing.T) {
	col := &Decimal{}
	_, err := col.parse("Decimal(9, 2)")
	require.NoError(t, err)

	// max int32 is 2147483647; scaled by 10^2 the max representable value is ~21474836.47
	overflow, err := decimal.NewFromString("21474836.48")
	require.NoError(t, err)
	err = col.AppendRow(overflow)
	assert.ErrorContains(t, err, "overflow")
}

func TestDecimal64OverflowReturnsError(t *testing.T) {
	col := &Decimal{}
	_, err := col.parse("Decimal(18, 2)")
	require.NoError(t, err)

	// max int64 is 9223372036854775807; scaled by 10^2 the max representable value is ~92233720368547758.07
	overflow, err := decimal.NewFromString("92233720368547758.08")
	require.NoError(t, err)
	err = col.AppendRow(overflow)
	assert.ErrorContains(t, err, "overflow")
}

func TestDecimal128OverflowReturnsError(t *testing.T) {
	col := &Decimal{}
	_, err := col.parse("Decimal(38, 0)")
	require.NoError(t, err)

	// 2^127 exceeds Decimal128 signed range
	big2_127 := new(big.Int).Lsh(big.NewInt(1), 127)
	overflow := decimal.NewFromBigInt(big2_127, 0)
	err = col.AppendRow(overflow)
	assert.ErrorContains(t, err, "overflow")
}

func TestDecimal256OverflowReturnsError(t *testing.T) {
	col := &Decimal{}
	_, err := col.parse("Decimal(76, 0)")
	require.NoError(t, err)

	// 2^255 exceeds Decimal256 signed range
	big2_255 := new(big.Int).Lsh(big.NewInt(1), 255)
	overflow := decimal.NewFromBigInt(big2_255, 0)
	err = col.AppendRow(overflow)
	assert.ErrorContains(t, err, "overflow")
}

func TestBigIntOverflowReturnsError(t *testing.T) {
	// Int128: signed 128-bit, max positive is 2^127-1
	col128 := &BigInt{size: 16, chType: "Int128", signed: true, col: &proto.ColInt128{}}

	big2_127 := new(big.Int).Lsh(big.NewInt(1), 127)
	err := col128.AppendRow(*big2_127)
	assert.ErrorContains(t, err, "overflow")
}

func TestBigIntValidValuesNoError(t *testing.T) {
	col128 := &BigInt{size: 16, chType: "Int128", signed: true, col: &proto.ColInt128{}}

	// 2^127 - 1 is the max valid Int128 value
	maxInt128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 127), big.NewInt(1))
	err := col128.AppendRow(*maxInt128)
	assert.NoError(t, err)

	// min valid Int128 value is -2^127
	minInt128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	err = col128.AppendRow(*minInt128)
	assert.NoError(t, err)
}

func TestBigIntNegativeOverflowReturnsError(t *testing.T) {
	col128 := &BigInt{size: 16, chType: "Int128", signed: true, col: &proto.ColInt128{}}

	// -2^127 - 1 is below the minimum Int128 value (-2^127)
	minInt128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))
	overflow := new(big.Int).Sub(minInt128, big.NewInt(1))
	err := col128.AppendRow(*overflow)
	assert.ErrorContains(t, err, "overflow")
}
