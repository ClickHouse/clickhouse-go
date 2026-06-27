package column

import (
	"strconv"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestLowCardinalityAppend_StringBulkMatchesRowByRow(t *testing.T) {
	data := []string{"a", "a", "b", "c", "a"}

	bulkCol, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	bulk := bulkCol.(*LowCardinality)
	_, err = bulk.Append(data)
	require.NoError(t, err)

	rowCol, err := Type("LowCardinality(String)").Column("test", nil)
	require.NoError(t, err)
	rowByRow := rowCol.(*LowCardinality)
	for _, v := range data {
		require.NoError(t, rowByRow.AppendRow(v))
	}

	require.Equal(t, len(data), bulk.Rows())
	require.Equal(t, bulk.Rows(), rowByRow.Rows())
	require.Equal(t, len(rowByRow.append.index), len(bulk.append.index))
	require.Equal(t, 3, len(bulk.append.index), "dictionary should hold the 3 distinct values")

	assert.Equal(t, encodeColumnBytes(t, rowByRow), encodeColumnBytes(t, bulk))
}

func TestLowCardinalityAppend_StringPtrEmbeddedNils(t *testing.T) {
	p := func(s string) *string { return &s }
	a1, a2 := p("a"), p("a")
	data := []*string{a1, nil, a2, p("b"), nil}

	col, err := Type("LowCardinality(Nullable(String))").Column("test", nil)
	require.NoError(t, err)
	lc := col.(*LowCardinality)

	_, err = lc.Append(data)
	require.NoError(t, err)

	require.Equal(t, 5, lc.Rows())
	assert.Equal(t, 2, len(lc.append.index),
		"appendLCPtr should dedupe by value, not by *string identity")

	var buf chproto.Buffer
	lc.Encode(&buf)

	deref := func(v any) any {
		if v == nil {
			return nil
		}
		if p, ok := v.(*string); ok {
			if p == nil {
				return nil
			}
			return *p
		}
		return v
	}
	assert.Equal(t, "a", deref(lc.Row(0, false)))
	assert.Nil(t, deref(lc.Row(1, false)))
	assert.Equal(t, "a", deref(lc.Row(2, false)))
	assert.Equal(t, "b", deref(lc.Row(3, false)))
	assert.Nil(t, deref(lc.Row(4, false)))
}

const lcBenchResetEveryN = 1024

func newBenchLowCardinalityColumn(b *testing.B, chType string) *LowCardinality {
	b.Helper()
	col, err := Type(chType).Column("bench", nil)
	if err != nil {
		b.Fatalf("parse %s: %v", chType, err)
	}
	lc, ok := col.(*LowCardinality)
	if !ok {
		b.Fatalf("expected *LowCardinality, got %T", col)
	}
	return lc
}

func benchLowCardinalityAppend(b *testing.B, col *LowCardinality, data any) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(data); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if (i+1)%lcBenchResetEveryN == 0 {
			col.Reset()
		}
	}
}

func makeLCStrings(n, unique int) []string {
	dict := make([]string, unique)
	for i := range dict {
		dict[i] = "value_" + strconv.Itoa(i)
	}
	out := make([]string, n)
	for i := range out {
		out[i] = dict[i%unique]
	}
	return out
}

func makeLCStringPtrs(n, unique int, nullEvery int) []*string {
	dict := make([]string, unique)
	for i := range dict {
		dict[i] = "value_" + strconv.Itoa(i)
	}
	out := make([]*string, n)
	for i := range out {
		if nullEvery > 0 && i%nullEvery == 0 {
			continue
		}
		s := dict[i%unique]
		out[i] = &s
	}
	return out
}

func BenchmarkLowCardinalityAppend_String_1000x26(b *testing.B) {
	col := newBenchLowCardinalityColumn(b, "LowCardinality(String)")
	benchLowCardinalityAppend(b, col, makeLCStrings(1000, 26))
}

func BenchmarkLowCardinalityAppend_String_1000x1000(b *testing.B) {
	// All-unique stress case: every row grows the dictionary.
	col := newBenchLowCardinalityColumn(b, "LowCardinality(String)")
	benchLowCardinalityAppend(b, col, makeLCStrings(1000, 1000))
}

func BenchmarkLowCardinalityAppend_String_10000x26(b *testing.B) {
	col := newBenchLowCardinalityColumn(b, "LowCardinality(String)")
	benchLowCardinalityAppend(b, col, makeLCStrings(10000, 26))
}

func BenchmarkLowCardinalityAppend_StringPtr_1000x26(b *testing.B) {
	col := newBenchLowCardinalityColumn(b, "LowCardinality(Nullable(String))")
	benchLowCardinalityAppend(b, col, makeLCStringPtrs(1000, 26, 0))
}

func BenchmarkLowCardinalityAppend_StringPtr_1000x26_WithNulls(b *testing.B) {
	col := newBenchLowCardinalityColumn(b, "LowCardinality(Nullable(String))")
	benchLowCardinalityAppend(b, col, makeLCStringPtrs(1000, 26, 4))
}

func BenchmarkLowCardinalityAppend_StringPtr_10000x26(b *testing.B) {
	col := newBenchLowCardinalityColumn(b, "LowCardinality(Nullable(String))")
	benchLowCardinalityAppend(b, col, makeLCStringPtrs(10000, 26, 0))
}
