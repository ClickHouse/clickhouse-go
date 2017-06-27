package clickhouse

import (
	"testing"
)

func Benchmark_ArrayWrite(b *testing.B) {
	var (
		buf   = wb(256 * 1024)
		array = Array([]string{"a", "b", "c", "d", "e", "f"})
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := array.write([]string{}, buf); err != nil {
			b.Fatal(err)
		}
		buf.free()
	}
}

func Benchmark_ArrayWriteSlice(b *testing.B) {
	var (
		buf   = wb(256 * 1024)
		array = Array([][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")})
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := array.write([][]byte{}, buf); err != nil {
			b.Fatal(err)
		}
		buf.free()
	}
}

func Benchmark_ArrayValue(b *testing.B) {
	array := Array([]string{"a", "b", "c", "d", "e", "f"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		array.Value()
	}
}
