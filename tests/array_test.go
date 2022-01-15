package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestArray(t *testing.T) {
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
		CREATE TABLE test_array (
			  Col1 Array(String)
			, Col2 Array(Array(UInt32))
			, Col3 Array(Array(Array(DateTime)))
		) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_array"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array"); assert.NoError(t, err) {
					var (
						timestamp = time.Now().Truncate(time.Second)
						col1Data  = []string{"A", "b", "c"}
						col2Data  = [][]uint32{
							[]uint32{1, 2},
							[]uint32{3, 87},
							[]uint32{33, 3, 847},
						}
						col3Data = [][][]time.Time{
							[][]time.Time{
								[]time.Time{
									timestamp,
									timestamp,
									timestamp,
									timestamp,
								},
							},
							[][]time.Time{
								[]time.Time{
									timestamp,
									timestamp,
									timestamp,
								},
								[]time.Time{
									timestamp,
									timestamp,
								},
							},
						}
					)
					for i := 0; i < 10; i++ {
						if err := batch.Append(col1Data, col2Data, col3Data); !assert.NoError(t, err) {
							return
						}
					}
					if assert.NoError(t, batch.Send()) {
						if rows, err := conn.Query(ctx, "SELECT * FROM test_array"); assert.NoError(t, err) {
							for rows.Next() {
								var (
									col1 []string
									col2 [][]uint32
									col3 [][][]time.Time
								)
								if err := rows.Scan(&col1, &col2, &col3); assert.NoError(t, err) {
									assert.Equal(t, col1Data, col1)
									assert.Equal(t, col2Data, col2)
									assert.Equal(t, col3Data, col3)
								}
							}
							if assert.NoError(t, rows.Close()) {
								assert.NoError(t, rows.Err())
							}
						}
					}
				}
			}
		}
	}
}
