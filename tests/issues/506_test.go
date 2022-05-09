package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test506(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			MaxOpenConns: 1,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if err != nil {
		assert.NoError(t, err)
	}

	const ddlA = `
		CREATE TABLE test_append_struct_a (
			  Col1  UInt32
			, Col2  String
			, Col3  Array(String)
			, Col4  Nullable(UInt8)
		) Engine Memory
		`

	const ddlB = `
		CREATE TABLE test_append_struct_b (
			  Col4  Array(UInt32)
			, Col3  Nullable(UInt8)
			, Col2  UInt32
			, Col1  String
		) Engine Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_append_struct_a")
		conn.Exec(ctx, "DROP TABLE test_append_struct_b")
	}()

	numQueries := 10
	numRows := 10000
	rowsPerQuery := numRows / numQueries
	ch := make(chan bool, numQueries)

	assert.NoError(t, conn.Exec(ctx, ddlA))
	assert.NoError(t, conn.Exec(ctx, ddlB))

	type dataA struct {
		Col1 uint32
		Col2 string
		Col3 []string
		Col4 *uint8
	}

	type dataB struct {
		Col4 []uint32
		Col3 *uint8
		Col2 uint32
		Col1 string
	}

	if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_append_struct_a"); assert.NoError(t, err) {
		for i := 0; i < numRows; i++ {
			str := fmt.Sprintf("Str_%d", i)
			err := batch.AppendStruct(&dataA{
				Col1: uint32(i),
				Col2: str,
				Col3: []string{str, "", str},
				Col4: nil,
			})
			if !assert.NoError(t, err) {
				return
			}
		}
		assert.NoError(t, batch.Send())
	}

	if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_append_struct_b"); assert.NoError(t, err) {
		for i := 0; i < numRows; i++ {
			str := fmt.Sprintf("Str_%d", i)
			err := batch.AppendStruct(&dataB{
				Col4: []uint32{uint32(i), uint32(i) + 1, uint32(i) + 2},
				Col3: nil,
				Col2: uint32(i),
				Col1: str,
			})
			if !assert.NoError(t, err) {
				return
			}
		}
		assert.NoError(t, batch.Send())
	}

	for i := 0; i < numQueries; i++ {
		go func(qNum int) {
			var results []dataA
			l := rowsPerQuery * qNum
			u := rowsPerQuery * (qNum + 1)
			query := fmt.Sprintf("SELECT * FROM test_append_struct_a WHERE Col1 >= %d and Col1 < %d ORDER BY Col1 ASC", l, u)
			if qNum%2 == 1 {
				query = fmt.Sprintf("SELECT * FROM test_append_struct_b WHERE Col2 >= %d and Col2 < %d ORDER BY Col2 ASC", l, u)
			}
			fmt.Printf("%d: %s\n", qNum, query)
			if err := conn.Select(ctx, &results, query); assert.NoError(t, err) {
				r := l
				for _, result := range results {
					str := fmt.Sprintf("Str_%d", r)
					assert.Equal(t, dataA{
						Col1: uint32(r),
						Col2: str,
						Col3: []string{str, "", str},
						Col4: nil,
					}, result)
					r++
				}
			}

			ch <- true
		}(i)
	}

	for numQueries > 0 {
		finished := <-ch
		assert.True(t, finished)
		numQueries--
	}

}
