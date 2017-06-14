package clickhouse

import (
	"io/ioutil"
	"testing"
)

func Benchmark_WriteInt32(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		writeInt32(ioutil.Discard, 42)
	}
}

func Benchmark_WriteUInt64(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		writeUInt64(ioutil.Discard, 42)
	}
}
