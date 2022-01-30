package issues

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestIssue472(t *testing.T) {
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
			CREATE TEMPORARY TABLE issue_472 (
				PodUID               UUID
				, EventType          String
				, ControllerRevision UInt8
				, Timestamp          DateTime
			)
		`
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_472"); assert.NoError(t, err) {
				podUID := uuid.New()
				if err := batch.Append(
					podUID,
					"Test",
					uint8(1),
					time.Now(),
				); !assert.NoError(t, err) {
					return
				}
				if err := batch.Send(); assert.NoError(t, err) {
					var records []struct {
						Timestamp time.Time
					}
					const query = `
							SELECT
								Timestamp
							FROM issue_472
							WHERE PodUID = $1
								AND (EventType = $2 or EventType = $3)
								AND ControllerRevision = $4 LIMIT 1`

					ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
						"max_block_size": 10,
					}))
					if err := conn.Select(ctx, &records, query, podUID, "Test", "", 1); assert.NoError(t, err) {
						t.Log(records)
					}
				}
			}
		}
	}
}
