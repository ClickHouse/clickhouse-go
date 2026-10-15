package column

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type namedInt64 int64

func TestIntegerAppendRowRejectsOverflow(t *testing.T) {
	cases := []struct {
		name  string
		typ   Type
		value any
	}{
		{name: "Int8 positive", typ: "Int8", value: int64(128)},
		{name: "Int8 negative", typ: "Int8", value: int64(-129)},
		{name: "UInt8 negative", typ: "UInt8", value: int64(-1)},
		{name: "UInt8 positive", typ: "UInt8", value: int64(256)},
		{name: "Int16 positive", typ: "Int16", value: int64(32768)},
		{name: "Int16 named source", typ: "Int16", value: namedInt64(32768)},
		{name: "Int16 negative", typ: "Int16", value: int64(-32769)},
		{name: "UInt16 negative", typ: "UInt16", value: int64(-1)},
		{name: "UInt8 negative int", typ: "UInt8", value: int(-1)},
		{name: "UInt16 positive", typ: "UInt16", value: uint32(65536)},
		{name: "Int32 positive", typ: "Int32", value: int64(1 << 31)},
		{name: "UInt32 negative", typ: "UInt32", value: int64(-1)},
		{name: "UInt32 positive", typ: "UInt32", value: uint64(1 << 32)},
		{name: "Int64 unsigned", typ: "Int64", value: uint64(math.MaxUint64)},
		{name: "UInt64 negative", typ: "UInt64", value: int64(-1)},
		{name: "UInt64 negative int", typ: "UInt64", value: int(-1)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col, err := tc.typ.Column("value", nil)
			require.NoError(t, err)

			err = col.AppendRow(tc.value)
			assert.ErrorContains(t, err, "overflow")
			assert.Zero(t, col.Rows(), "overflowing values must not be appended")
		})
	}
}

func TestIntegerArrayAppendRowRejectsOverflow(t *testing.T) {
	col, err := Type("Array(Int8)").Column("values", nil)
	require.NoError(t, err)

	err = col.AppendRow([]int64{128})
	assert.ErrorContains(t, err, "overflow")
}

func TestIntegerAppendRowAcceptsBoundaryValues(t *testing.T) {
	cases := []struct {
		name  string
		typ   Type
		value any
		want  any
	}{
		{name: "Int8 max", typ: "Int8", value: int64(math.MaxInt8), want: int8(math.MaxInt8)},
		{name: "Int8 min", typ: "Int8", value: int64(math.MinInt8), want: int8(math.MinInt8)},
		{name: "UInt8 max", typ: "UInt8", value: int16(math.MaxUint8), want: uint8(math.MaxUint8)},
		{name: "Int16 max from unsigned", typ: "Int16", value: uint16(math.MaxInt16), want: int16(math.MaxInt16)},
		{name: "UInt16 max", typ: "UInt16", value: uint32(math.MaxUint16), want: uint16(math.MaxUint16)},
		{name: "Int32 min", typ: "Int32", value: int64(math.MinInt32), want: int32(math.MinInt32)},
		{name: "UInt32 max", typ: "UInt32", value: uint64(math.MaxUint32), want: uint32(math.MaxUint32)},
		{name: "Int64 max from unsigned", typ: "Int64", value: uint64(math.MaxInt64), want: int64(math.MaxInt64)},
		{name: "UInt64 max from signed", typ: "UInt64", value: int64(math.MaxInt64), want: uint64(math.MaxInt64)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			col, err := tc.typ.Column("value", nil)
			require.NoError(t, err)
			require.NoError(t, col.AppendRow(tc.value))
			assert.Equal(t, tc.want, col.Row(0, false))
		})
	}
}
