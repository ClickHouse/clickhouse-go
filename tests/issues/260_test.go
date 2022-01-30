package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue260(t *testing.T) {
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
		CREATE TEMPORARY TABLE issue_260 (
			Col1 Nullable(DateTime('UTC'))
		)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_260"); assert.NoError(t, err) {
				if err := batch.Append(nil); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var col1 *time.Time
						if err := conn.QueryRow(ctx, "SELECT * FROM issue_260").Scan(&col1); assert.NoError(t, err) {
							assert.Nil(t, col1)
						}
					}
				}
			}
		}
	}
}
