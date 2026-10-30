package column

import (
	"bytes"
	"math"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLowCardinalityFloatBits(t *testing.T) {
	for _, inner := range []string{"Float32", "Float64", "Nullable(Float32)", "Nullable(Float64)"} {
		t.Run(inner, func(t *testing.T) {
			values := []any{float64(0), math.Copysign(0, -1), math.NaN(), math.NaN(), math.Inf(1), math.Inf(-1), 1.5}
			if inner == "Float32" || inner == "Nullable(Float32)" {
				for i, value := range values {
					values[i] = float32(value.(float64))
				}
				values = append(values, math.Float32frombits(0x7fc00013))
			} else {
				values = append(values, math.Float64frombits(0x7ff8000000000013))
			}
			if inner == "Nullable(Float32)" || inner == "Nullable(Float64)" {
				values = append(values, nil)
			}
			col, err := Type("LowCardinality("+inner+")").Column("test", nil)
			require.NoError(t, err)
			for _, columnar := range []bool{false, true} {
				col.Reset()
				if columnar {
					_, err = col.Append(values)
					require.NoError(t, err)
				} else {
					for _, value := range values {
						require.NoError(t, col.AppendRow(value))
					}
				}
				// The repeated NaN shares an entry, while different payloads do not.
				// Reserved default/null slots offset the duplicate and nil rows.
				assert.Equal(t, len(values), col.(*LowCardinality).index.Rows())
				var buf chproto.Buffer
				col.Encode(&buf)
				decoded, err := col.Type().Column("test", nil)
				require.NoError(t, err)
				require.NoError(t, decoded.Decode(chproto.NewReader(bytes.NewReader(buf.Buf)), len(values)))
				for i, value := range values {
					got := decoded.Row(i, false)
					switch want := value.(type) {
					case float32:
						if ptr, ok := got.(*float32); ok {
							got = *ptr
						}
						require.NotNil(t, got, "row %d", i)
						assert.Equal(t, math.Float32bits(want), math.Float32bits(got.(float32)), "row %d", i)
					case float64:
						if ptr, ok := got.(*float64); ok {
							got = *ptr
						}
						require.NotNil(t, got, "row %d", i)
						assert.Equal(t, math.Float64bits(want), math.Float64bits(got.(float64)), "row %d", i)
					default:
						assert.Nil(t, got)
					}
				}
			}
		})
	}
}

func TestLowCardinalityAppendAnySlice(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 10 {
		err := lc.AppendRow("value_" + string(rune('A'+i)))
		assert.NoError(t, err)
	}

	assert.Equal(t, 10, lc.Rows())
}

func TestLowCardinalityAppendAnySliceManyRows(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 500 {
		err := lc.AppendRow("value_" + string(rune('A'+i%26)))
		assert.NoError(t, err)
	}

	assert.Equal(t, 500, lc.Rows())
}

func TestLowCardinalityResetAfterEncode(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	require.NotNil(t, lc.append.index)

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	lc.Reset()

	require.NotNil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
}

func TestLowCardinalityAppendAfterEncodeWithoutReset(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	require.NotNil(t, lc.append.index)

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
}

func TestLowCardinalityEncodeThenResetThenAppend(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for range 10 {
		err := lc.AppendRow("value")
		require.NoError(t, err)
	}

	var buf chproto.Buffer
	lc.Encode(&buf)

	assert.Nil(t, lc.append.index)

	lc.Reset()

	require.NotNil(t, lc.append.index)

	err = lc.AppendRow("new_value")
	assert.NoError(t, err)
	assert.Equal(t, 1, lc.Rows())
}

func TestLowCardinalityAppendManyRowsWithoutPanic(t *testing.T) {
	col, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)

	lc, ok := col.(*LowCardinality)
	require.True(t, ok)

	for i := range 1000 {
		err := lc.AppendRow("value_" + string(rune('A'+i%26)))
		assert.NoError(t, err, "Failed at row %d", i)
	}

	assert.Equal(t, 1000, lc.Rows())
}
