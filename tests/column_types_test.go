package tests

import (
	"context"
	"reflect"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestColumnTypes(t *testing.T) {
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
	const query = `
		SELECT
			  CAST(1   AS UInt8)  AS Col1
			, CAST('X' AS String) AS Col2
	`
	if assert.NoError(t, err) {
		if rows, err := conn.Query(ctx, query); assert.NoError(t, err) {
			if types := rows.ColumnTypes(); assert.Len(t, types, 2) {
				for i, v := range types {
					switch i {
					case 0:
						if assert.False(t, v.Nullable()) {
							assert.Equal(t, "Col1", v.Name())
							assert.Equal(t, reflect.TypeOf(uint8(0)), v.ScanType())
							assert.Equal(t, "UInt8", v.DatabaseTypeName())
						}
					case 1:
						if assert.False(t, v.Nullable()) {
							assert.Equal(t, "Col2", v.Name())
							assert.Equal(t, reflect.TypeOf(""), v.ScanType())
							assert.Equal(t, "String", v.DatabaseTypeName())
						}

					}
				}
			}
		}
	}
}
