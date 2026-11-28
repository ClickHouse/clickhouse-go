package column

import (
	"strconv"
	"testing"
)

func newBenchArrayStringColumn(b *testing.B, rows, elems int) *Array {
	b.Helper()
	col, err := Type("Array(String)").Column("bench", &ServerContext{})
	if err != nil {
		b.Fatalf("Array(String) column: %v", err)
	}
	row := make([]string, elems)
	for i := range row {
		row[i] = "frame_" + strconv.Itoa(i)
	}
	for i := 0; i < rows; i++ {
		if err := col.AppendRow(row); err != nil {
			b.Fatalf("AppendRow: %v", err)
		}
	}
	return col.(*Array)
}

// BenchmarkArrayStringScanRow scans Array(String) rows into []string (#1747).
func BenchmarkArrayStringScanRow(b *testing.B) {
	const rows, elems = 64, 100
	col := newBenchArrayStringColumn(b, rows, elems)
	var dest []string
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.ScanRow(&dest, i%rows); err != nil {
			b.Fatalf("ScanRow: %v", err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*elems), "ns/elem")
}
