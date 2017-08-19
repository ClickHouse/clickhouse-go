package binary

import (
	"fmt"
	"io/ioutil"
	"math"
	"testing"
	"time"
)

func Benchmark_Encoder_Uvarint(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Uvarint(math.MaxUint64)
	}
}

func Benchmark_Encoder_Boolean(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Bool(true)
	}
}

func Benchmark_Encoder_Int8(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Int8(127)
	}
}

func Benchmark_Encoder_Int16(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Int16(32767)
	}
}

func Benchmark_Encoder_Int32(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Int32(2147483647)
	}
}

func Benchmark_Encoder_Int64(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Int64(9223372036854775807)
	}
}

func Benchmark_Encoder_UInt8(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.UInt8(255)
	}
}

func Benchmark_Encoder_UInt16(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.UInt16(65535)
	}
}

func Benchmark_Encoder_UInt32(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.UInt32(4294967295)
	}
}

func Benchmark_Encoder_UInt64(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.UInt64(18446744073709551615)
	}
}

func Benchmark_Encoder_Float32(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Float32(2147483647)
	}
}

func Benchmark_Encoder_Float64(b *testing.B) {
	encoder := NewEncoder(ioutil.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.Float64(2147483647)
	}
}

func Benchmark_Encoder_String(b *testing.B) {
	var (
		str     = fmt.Sprintf("str_%d", time.Now().Unix())
		encoder = NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.String(str)
	}
}

func Benchmark_Encoder_RawString(b *testing.B) {
	var (
		str     = []byte(fmt.Sprintf("str_%d", time.Now().Unix()))
		encoder = NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder.RawString(str)
	}
}
