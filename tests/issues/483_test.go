package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue483(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	const ddl = `
		CREATE TABLE issue_483
		(
			example_id UInt8,
			steps Nested(
				  duration UInt8,
				  result Nested(
						duration UInt64,
						error_message Nullable(String),
						status UInt8
					),
				  keyword String
				),
			status UInt8
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_483")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_483 (example_id)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(uint8(1)))
	require.NoError(t, batch.Send())
	var (
		col1 uint8
		col2 []uint8   // steps.duration
		col3 [][][]any // steps.result
		col4 []string  //  steps.keyword
		col5 uint8
	)
	require.NoError(t, conn.QueryRow(ctx, `SELECT * FROM issue_483`).Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Equal(t, uint8(1), col1)
	assert.Equal(t, []uint8{}, col2)
}
