package column

import (
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/lib/binary"
)

func Benchmark_Column_Int8(b *testing.B) {
	var (
		column  Int8
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, int8(math.MaxInt8)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Int16(b *testing.B) {
	var (
		column  Int16
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, int16(math.MaxInt16)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Int32(b *testing.B) {
	var (
		column  Int32
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, int32(math.MaxInt32)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Int64(b *testing.B) {
	var (
		column  Int64
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, int64(math.MaxInt64)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_UInt8(b *testing.B) {
	var (
		column  UInt8
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, uint8(math.MaxUint8)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_UInt16(b *testing.B) {
	var (
		column  UInt16
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, uint16(math.MaxUint16)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_UInt32(b *testing.B) {
	var (
		column  UInt32
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, uint32(math.MaxUint32)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_UInt64(b *testing.B) {
	var (
		column  UInt64
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, uint64(math.MaxUint64)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Float32(b *testing.B) {
	var (
		column  Float32
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, float32(math.MaxInt32)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Float64(b *testing.B) {
	var (
		column  Float64
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, float64(math.MaxInt64)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_String(b *testing.B) {
	var (
		column  String
		str     = fmt.Sprintf("str_%d", time.Now().Unix())
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, str); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_FixedString(b *testing.B) {
	var (
		str       = []byte(fmt.Sprintf("str_%d", time.Now().Unix()))
		column, _ = Factory("", "FixedString(14)", time.Local)
		encoder   = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, str); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Enum8(b *testing.B) {
	var (
		column, _ = Factory("", "Enum8('A'=1, 'B'=2, 'C'=3)", time.Local)
		encoder   = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, "B"); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Enum16(b *testing.B) {
	var (
		column, _ = Factory("", "Enum16('A'=1,'B'=2,'C'=3)", time.Local)
		encoder   = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, "B"); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_Date(b *testing.B) {
	var (
		column, _ = Factory("", "Date", time.Local)
		encoder   = binary.NewEncoder(ioutil.Discard)
		timeNow   = time.Now()
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, timeNow); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_DateTime(b *testing.B) {
	var (
		column, _ = Factory("", "DateTime", time.Local)
		encoder   = binary.NewEncoder(ioutil.Discard)
		timeNow   = time.Now()
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, timeNow); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_UUID(b *testing.B) {
	var (
		column  UUID
		encoder = binary.NewEncoder(ioutil.Discard)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, "0492351a-3cb1-4cb5-855f-e0508145a54c"); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_IP(b *testing.B) {
	var (
		encoder   = binary.NewEncoder(ioutil.Discard)
		column, _ = Factory("", "IPv4", time.Local)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, net.ParseIP("1.2.3.4")); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Column_IP_STRING(b *testing.B) {
	var (
		encoder   = binary.NewEncoder(ioutil.Discard)
		column, _ = Factory("", "IPv4", time.Local)
	)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := column.Write(encoder, "1.2.3.4"); err != nil {
			b.Fatal(err)
		}
	}
}
