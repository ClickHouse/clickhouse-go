package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue406(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 21, 9); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TEMPORARY TABLE issue_406 (
				Col1 Tuple(Array(Int32), Array(Int32))
			)
		`

		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_406"); assert.NoError(t, err) {
				if err := batch.Append(
					[]interface{}{
						[]int32{1, 2, 3, 4, 5},
						[]int32{5, 1, 2, 3, 4},
					},
				); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var col1 []interface{}
						if err := conn.QueryRow(ctx, "SELECT * FROM issue_406").Scan(&col1); assert.NoError(t, err) {
							assert.Equal(t, []interface{}{
								[]int32{1, 2, 3, 4, 5},
								[]int32{5, 1, 2, 3, 4},
							}, col1)
						}
					}
				}
			}
		}
	}
}
