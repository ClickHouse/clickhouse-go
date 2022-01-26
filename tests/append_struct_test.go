package tests

import (
	"context"
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
			  Col1 UInt8
			, Col2 String
			, Col3 Array(String)
		) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_append_struct"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_append_struct"); assert.NoError(t, err) {
					type data struct {
						Col1 uint8
						Col2 string
						Col3 []string
					}
					err := batch.AppendStruct(&data{
						Col1: 42,
						Col3: []string{"A", "B", "C"},
					})
					if assert.NoError(t, err) && assert.NoError(t, batch.Send()) {
						var result data
						if err := conn.QueryRow(ctx, "SELECT * FROM test_append_struct").ScanStruct(&result); assert.NoError(t, err) {
							if assert.Empty(t, result.Col2) {
								assert.Equal(t, uint8(42), result.Col1)
								assert.Equal(t, []string{"A", "B", "C"}, result.Col3)
							}
						}
					}
				}
			}
		}
	}
}
