package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestAppendStruct(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_append_struct (
			  HCol1 UInt8
			, HCol2 String
			, HCol3 Array(Nullable(String))
			, Col1  UInt8
			, Col2  String
			, Col3  Array(String)
		) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_append_struct"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_append_struct"); assert.NoError(t, err) {
					type header struct {
						Col1 uint8     `ch:"HCol1"`
						Col2 *string   `ch:"HCol2"`
						Col3 []*string `ch:"HCol3"`
					}
					type data struct {
						header
						Col1 uint8
						Col2 string
						Col3 []string
					}
					for i := 0; i < 100; i++ {
						str := fmt.Sprintf("Str_%d", i)
						err := batch.AppendStruct(&data{
							header: header{
								Col1: uint8(i),
								Col2: &str,
								Col3: []*string{&str, nil, &str},
							},
							Col1: uint8(i + 1),
							Col3: []string{"A", "B", "C", fmt.Sprint(i)},
						})
						if !assert.NoError(t, err) {
							return
						}
					}

					if assert.NoError(t, batch.Send()) {
						for i := 0; i < 100; i++ {
							var result data
							if err := conn.QueryRow(ctx, "SELECT * FROM test_append_struct WHERE HCol1 = $1", i).ScanStruct(&result); assert.NoError(t, err) {
								str := fmt.Sprintf("Str_%d", i)
								h := header{
									Col1: uint8(i),
									Col2: &str,
									Col3: []*string{&str, nil, &str},
								}
								assert.Equal(t, h, result.header)
								if assert.Empty(t, result.Col2) {
									assert.Equal(t, uint8(i+1), result.Col1)
									assert.Equal(t, []string{"A", "B", "C", fmt.Sprint(i)}, result.Col3)
								}
							}
						}
						var results []data
						if err := conn.Select(ctx, &results, "SELECT * FROM test_append_struct ORDER BY HCol1"); assert.NoError(t, err) {
							for i, result := range results {
								str := fmt.Sprintf("Str_%d", i)
								h := header{
									Col1: uint8(i),
									Col2: &str,
									Col3: []*string{&str, nil, &str},
								}
								assert.Equal(t, h, result.header)
								if assert.Empty(t, result.Col2) {
									assert.Equal(t, uint8(i+1), result.Col1)
									assert.Equal(t, []string{"A", "B", "C", fmt.Sprint(i)}, result.Col3)
								}
							}
						}
					}
				}
			}
		}
	}
}
