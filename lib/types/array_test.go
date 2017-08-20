package types

import (
	"testing"
)

func Benchmark_Types_ArrayAsDriverValue(b *testing.B) {
	array := NewArray([]string{"A", "B", "C"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := array.Value(); err != nil {
			b.Fatal(err)
		}
	}
}
