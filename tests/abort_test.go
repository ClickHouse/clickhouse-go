package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestAbort(t *testing.T) {
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
			MaxOpenConns: 1,
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TABLE test_abort (
			Col1 UInt8
		) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_abort")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_abort"); assert.NoError(t, err) {
				if assert.NoError(t, batch.Abort()) {
					if err := batch.Abort(); assert.Error(t, err) {
						assert.Equal(t, clickhouse.ErrBatchAlreadySent, err)
					}
				}
			}
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_abort"); assert.NoError(t, err) {
				if assert.NoError(t, batch.Append(uint8(1))) && assert.NoError(t, batch.Send()) {
					var col1 uint8
					if err := conn.QueryRow(ctx, "SELECT * FROM test_abort").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, uint8(1), col1)
					}
				}
			}
		}
	}
}
