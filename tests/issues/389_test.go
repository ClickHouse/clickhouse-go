package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue389(t *testing.T) {
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
		if err := checkMinServerVersion(conn, 20, 3); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TEMPORARY TABLE issue_389 (
				    Col1 DateTime64(3, 'America/New_York')
			)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_389"); assert.NoError(t, err) {
				if err := batch.Append(int64(1625128291293)); assert.NoError(t, err) {
					if err := batch.Send(); assert.NoError(t, err) {
						var col1 time.Time

						if err := conn.QueryRow(ctx, "SELECT * FROM issue_389").Scan(&col1); assert.NoError(t, err) {
							if assert.Equal(t, "America/New_York", col1.Location().String()) {
								assert.Equal(t, "2021-07-01 04:31:31.293 -0400 EDT", col1.String())
							}
						}
					}
				}
			}
		}
	}
}
