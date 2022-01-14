package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestBool(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 12); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_bool (
				    Col1 Bool
				  , Col2 Bool
			) Engine Memory
		`
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS test_bool"); assert.NoError(t, err) {
			if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
				if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bool"); assert.NoError(t, err) {
					if err := batch.Append(true, false); assert.NoError(t, err) {
						if err := batch.Send(); assert.NoError(t, err) {
							var (
								col1 bool
								col2 bool
							)
							if err := conn.QueryRow(ctx, "SELECT * FROM test_bool").Scan(&col1, &col2); assert.NoError(t, err) {
								assert.Equal(t, true, col1)
								assert.Equal(t, false, col2)
							}
						}
					}
				}
			}
		}
	}
}
