package column

import (
	"strconv"
	"testing"
)

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
