package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestNested(t *testing.T) {
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
			//	Debug: true,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_nested")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested"); assert.NoError(t, err) {
				var (
					col1Data = []uint8{1, 2, 3}
					col2Data = []uint8{10, 20, 30}
					col3Data = []uint8{101, 201, 230} // Col2.Col1_N2
					col4Data = [][][]interface{}{
						[][]interface{}{
							[]interface{}{uint8(1), uint8(2)},
						},
						[][]interface{}{
							[]interface{}{uint8(1), uint8(2)},
						},
						[][]interface{}{
							[]interface{}{uint8(1), uint8(2)},
						},
					}
				)
				if err := batch.Append(col1Data, col2Data, col3Data, col4Data); assert.NoError(t, err) {
					if assert.NoError(t, batch.Send()) {
						var (
							col1 []uint8
							col2 []uint8
							col3 []uint8
							col4 [][][]interface{}
						)
						if err := conn.QueryRow(ctx, "SELECT * FROM test_nested").Scan(&col1, &col2, &col3, &col4); assert.NoError(t, err) {
							assert.Equal(t, col1Data, col1)
							assert.Equal(t, col2Data, col2)
							assert.Equal(t, col3Data, col3)
							assert.Equal(t, col4Data, col4)
						}
					}
				}
			}
		}
	}
}
