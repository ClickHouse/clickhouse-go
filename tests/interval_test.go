package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestInterval(t *testing.T) {
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
		const query = `
		SELECT
			  INTERVAL 1 SECOND
			, INTERVAL 4 SECOND
			, INTERVAL 1 MINUTE
			, INTERVAL 5 MINUTE
		`
		var (
			col1 string
			col2 string
			col3 string
			col4 string
		)

		err := conn.QueryRow(ctx, query).Scan(
			&col1,
			&col2,
			&col3,
			&col4,
		)

		if assert.NoError(t, err) {
			assert.Equal(t, "1 Second", col1)
			assert.Equal(t, "4 Seconds", col2)
			assert.Equal(t, "1 Minute", col3)
			assert.Equal(t, "5 Minutes", col4)
		}
	}
}
