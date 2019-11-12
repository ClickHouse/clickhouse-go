package data

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/lib/column"
)

func Benchmark_BlockWriteArrayWithLen4(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1
	// blocks.Columns = 1
	c, err := column.Factory("test", "Int32", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		block.WriteArrayLen(4, 0, 1)
		for i := 0; i > 4; i++ {
			block.WriteInt32(0, 1)
		}
	}
}

func Benchmark_BlockWriteArray4(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1
	// blocks.Columns = 1
	c, err := column.Factory("test", "Int32", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		block.WriteArray(0, []int32{0, 1, 2, 3})
	}
}
