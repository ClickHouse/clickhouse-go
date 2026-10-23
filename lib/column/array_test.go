package column

import (
	"strconv"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func encodeColumnBytes(t *testing.T, col Interface) []byte {
	t.Helper()
	var buf chproto.Buffer
	col.Encode(&buf)
	out := make([]byte, len(buf.Buf))
	copy(out, buf.Buf)
	return out
}

func newArrayColumn(t *testing.T, chType string) *Array {
	t.Helper()
	col, err := Type(chType).Column("test", nil)
	require.NoError(t, err)
	arr, ok := col.(*Array)
	require.True(t, ok, "expected *Array, got %T", col)
	return arr
}

func TestArrayAppendBulk_Int64_MatchesRowByRow(t *testing.T) {
	rows := [][]int64{{1, 2, 3}, {}, {4, 5}, {6}}

	bulk := newArrayColumn(t, "Array(Int64)")
	_, err := bulk.Append(rows)
	require.NoError(t, err)

	rowByRow := newArrayColumn(t, "Array(Int64)")
	for _, r := range rows {
		require.NoError(t, rowByRow.AppendRow(r))
	}

	require.Equal(t, len(rows), bulk.Rows())
	require.Equal(t, bulk.Rows(), rowByRow.Rows())
	for i := range rows {
		assert.Equal(t, rowByRow.Row(i, false), bulk.Row(i, false), "row %d", i)
	}
	assert.Equal(t, encodeColumnBytes(t, rowByRow), encodeColumnBytes(t, bulk))
}

func TestArrayAppendBulk_ConvertibleIntSlice(t *testing.T) {
	rows := [][]int{{1, 2, 3}, {4, 5}}

	col := newArrayColumn(t, "Array(Int64)")
	_, err := col.Append(rows)
	require.NoError(t, err)

	require.Equal(t, 2, col.Rows())
	require.Equal(t, 5, col.values.Rows())
	assert.Equal(t, []int64{1, 2, 3}, col.Row(0, false))
	assert.Equal(t, []int64{4, 5}, col.Row(1, false))
}

func TestArrayAppendRow_ConvertibleIntSlice(t *testing.T) {
	rows := [][]int{{1, 2, 3}, {4, 5}}

	col := newArrayColumn(t, "Array(Int64)")
	for _, r := range rows {
		require.NoError(t, col.AppendRow(r))
	}

	require.Equal(t, 2, col.Rows())
	require.Equal(t, 5, col.values.Rows())
	assert.Equal(t, []int64{1, 2, 3}, col.Row(0, false))
	assert.Equal(t, []int64{4, 5}, col.Row(1, false))
}

func TestArrayAppendBulk_NullableInt64_MixedNils(t *testing.T) {
	p := func(v int64) *int64 { return &v }
	rows := [][]*int64{{p(1), nil, p(2)}, {nil}, {p(3), p(4)}}

	bulk := newArrayColumn(t, "Array(Nullable(Int64))")
	_, err := bulk.Append(rows)
	require.NoError(t, err)

	rowByRow := newArrayColumn(t, "Array(Nullable(Int64))")
	for _, r := range rows {
		require.NoError(t, rowByRow.AppendRow(r))
	}

	require.Equal(t, len(rows), bulk.Rows())
	require.Equal(t, bulk.Rows(), rowByRow.Rows())

	for i := range rows {
		gotBulk, gotRow := bulk.Row(i, false), rowByRow.Row(i, false)
		assert.Equal(t, gotRow, gotBulk, "row %d", i)

		got, ok := gotBulk.([]*int64)
		require.True(t, ok, "expected []*int64 for row %d, got %T", i, gotBulk)
		require.Equal(t, len(rows[i]), len(got))
		for j, want := range rows[i] {
			if want == nil {
				assert.Nil(t, got[j], "row %d col %d", i, j)
				continue
			}
			require.NotNil(t, got[j], "row %d col %d", i, j)
			assert.Equal(t, *want, *got[j], "row %d col %d", i, j)
		}
	}

	assert.Equal(t, encodeColumnBytes(t, rowByRow), encodeColumnBytes(t, bulk))
}

func TestArrayAppendBulk_StringMatchesRowByRow(t *testing.T) {
	rows := [][]string{{"a", "b"}, {}, {"c"}}

	bulk := newArrayColumn(t, "Array(String)")
	_, err := bulk.Append(rows)
	require.NoError(t, err)

	rowByRow := newArrayColumn(t, "Array(String)")
	for _, r := range rows {
		require.NoError(t, rowByRow.AppendRow(r))
	}

	require.Equal(t, len(rows), bulk.Rows())
	require.Equal(t, bulk.Rows(), rowByRow.Rows())
	for i := range rows {
		assert.Equal(t, rowByRow.Row(i, false), bulk.Row(i, false), "row %d", i)
	}
	assert.Equal(t, encodeColumnBytes(t, rowByRow), encodeColumnBytes(t, bulk))
}

const arrayBenchResetEveryN = 1024

func newBenchArrayColumn(b *testing.B, chType string) *Array {
	b.Helper()
	col, err := Type(chType).Column("bench", nil)
	if err != nil {
		b.Fatalf("parse %s: %v", chType, err)
	}
	arr, ok := col.(*Array)
	if !ok {
		b.Fatalf("expected *Array, got %T", col)
	}
	return arr
}

func benchArrayAppend(b *testing.B, col *Array, data any) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Append(data); err != nil {
			b.Fatalf("Append: %v", err)
		}
		if (i+1)%arrayBenchResetEveryN == 0 {
			col.Reset()
		}
	}
}

func makeInt64Rows(rows, perRow int) [][]int64 {
	out := make([][]int64, rows)
	for i := range out {
		row := make([]int64, perRow)
		for j := range row {
			row[j] = int64(i*perRow + j)
		}
		out[i] = row
	}
	return out
}

func makeStringRows(rows, perRow int) [][]string {
	out := make([][]string, rows)
	for i := range out {
		row := make([]string, perRow)
		for j := range row {
			row[j] = "v_" + strconv.Itoa(i*perRow+j)
		}
		out[i] = row
	}
	return out
}

func makeNullableInt64Rows(rows, perRow int) [][]*int64 {
	out := make([][]*int64, rows)
	for i := range out {
		row := make([]*int64, perRow)
		for j := range row {
			if j%4 == 0 {
				continue
			}
			v := int64(i*perRow + j)
			row[j] = &v
		}
		out[i] = row
	}
	return out
}

func BenchmarkArrayAppend_Int64_100x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(Int64)")
	benchArrayAppend(b, col, makeInt64Rows(100, 10))
}

func BenchmarkArrayAppend_Int64_1000x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(Int64)")
	benchArrayAppend(b, col, makeInt64Rows(1000, 10))
}

func BenchmarkArrayAppend_Int64_100x100(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(Int64)")
	benchArrayAppend(b, col, makeInt64Rows(100, 100))
}

func BenchmarkArrayAppend_String_100x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(String)")
	benchArrayAppend(b, col, makeStringRows(100, 10))
}

func BenchmarkArrayAppend_String_1000x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(String)")
	benchArrayAppend(b, col, makeStringRows(1000, 10))
}

func BenchmarkArrayAppend_NullableInt64_100x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(Nullable(Int64))")
	benchArrayAppend(b, col, makeNullableInt64Rows(100, 10))
}

func BenchmarkArrayAppend_NullableInt64_1000x10(b *testing.B) {
	col := newBenchArrayColumn(b, "Array(Nullable(Int64))")
	benchArrayAppend(b, col, makeNullableInt64Rows(1000, 10))
}
