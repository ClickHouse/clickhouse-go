package data

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/lib/column"
)

func Benchmark_BlockWriteArrayWithLen4(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1

	c, err := column.Factory("test", "Int32", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()
	input := []int32{
		1, 2, 3,
	}
	for i := 0; i < b.N; i++ {
		block.WriteArrayLen(len(input), 0, 1)
		for _, val := range input {
			block.WriteInt32(0, val)
		}
	}
}

func Benchmark_BlockWriteArray4(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1

	c, err := column.Factory("test", "Int32", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()
	input := []int32{
		1, 2, 3,
	}
	for i := 0; i < b.N; i++ {
		block.WriteArray(0, input)
	}
}

func Benchmark_BlockWriteArrayTwoLevelWithLen(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1

	c, err := column.Factory("test", "Array(Int32)", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()
	input := [][]int32{
		{
			1, 2, 3,
		},
		{
			4, 5, 6,
		},
	}
	for i := 0; i < b.N; i++ {
		block.WriteArrayLen(len(input), 0, 1)
		for _, val := range input {
			block.WriteArrayLen(len(val), 0, 2)
			for _, v := range val {
				block.WriteInt32(0, v)
			}
		}

	}
}

func Benchmark_BlockWriteArrayTwoLevel(b *testing.B) {

	block := &Block{}
	block.NumColumns = 1

	c, err := column.Factory("test", "Array(Int32)", nil)
	if err != nil {
		b.Fatal(err)
	}
	block.Columns = append(block.Columns, c)
	block.Reserve()
	b.ReportAllocs()
	input := [][]int32{
		{
			1, 2, 3,
		},
		{
			4, 5, 6,
		},
	}
	for i := 0; i < b.N; i++ {
		block.WriteArray(0, input)
	}
}
