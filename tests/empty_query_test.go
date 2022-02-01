package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestEmptyQuery(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	if assert.NoError(t, err) {
		const ddl = `
		CREATE TEMPORARY TABLE test_empty_query (
			Col1 UInt8
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
			defer cancel()
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_empty_query"); assert.NoError(t, err) {
				assert.NoError(t, batch.Send())
			}
		}
	}
}
