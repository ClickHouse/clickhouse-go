package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue412(t *testing.T) {
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
			CREATE TEMPORARY TABLE issue_412 (
				Col1 SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
			)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_412"); assert.NoError(t, err) {
				datetime := time.Now().Truncate(time.Millisecond)
				if err := batch.Append(datetime); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var col1 time.Time
					if err := conn.QueryRow(ctx, "SELECT * FROM issue_412").Scan(&col1); assert.NoError(t, err) {
						assert.Equal(t, datetime.UnixNano(), col1.UnixNano())
					}
				}
			}
		}
	}
}
