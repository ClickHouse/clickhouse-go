package issues

import (
	"context"
	"testing"
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

func TestIssue472(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE issue_472 (
				PodUID               UUID
				, EventType          String
				, ControllerRevision UInt8
				, Timestamp          DateTime
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_472")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_472")
	require.NoError(t, err)
	podUID := uuid.New()
	require.NoError(t, batch.Append(
		podUID,
		"Test",
		uint8(1),
		time.Now(),
	))
	require.NoError(t, batch.Send())
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

	ctx = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}))
	require.NoError(t, conn.Select(ctx, &records, query, podUID, "Test", "", 1))
	t.Log(records)
}
