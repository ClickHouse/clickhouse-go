package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1638_NullableJSON(t *testing.T) {
	ctx := context.Background()

	conn, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, err, "open clickhouse")

	// Fresh table for the test.
	_ = conn.Exec(ctx, `DROP TABLE IF EXISTS test_nullable_json`)
	err = conn.Exec(ctx, `
		CREATE TABLE test_nullable_json
		(
			id Int32,
			payload Nullable(JSON)
		)
		ENGINE = MergeTree
		ORDER BY (id)
	`)
	require.NoError(t, err, "create table")

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO test_nullable_json (id, payload) VALUES (?, ?)`)
	require.NoError(t, err, "prepare batch")
	var nilMap map[string]string
	require.NoError(t, batch.Append(1, nilMap))
	require.NoError(t, batch.Send(), "batch send")

	var retrievedPayload map[string]string
	err = conn.QueryRow(ctx, `SELECT payload FROM test_nullable_json WHERE id = 1`, 2).Scan(&retrievedPayload)
	require.NoError(t, err, "select payload")

	// retrievedPayload should be nil given we inserted NULL value
	require.Nil(t, retrievedPayload, "payload for id=2 should be NULL (batched insert). Got: %#v", retrievedPayload)
}
