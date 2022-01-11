package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestNothing(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
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
	if assert.NoError(t, err) {
		if rows, err := conn.Query(context.Background(), "SELECT NULL FROM system.numbers_mt LIMIT 10"); assert.NoError(t, err) {
			var count int
			for rows.Next() {
				var nothing []struct{}
				if !assert.NoError(t, rows.Scan(&nothing)) {
					return
				}
				count++
			}
			rows.Close()
			if assert.NoError(t, rows.Err()) {
				assert.Equal(t, 10, count)
			}
		}
	}
}
