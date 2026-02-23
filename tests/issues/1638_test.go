package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1638_NullableJSON(t *testing.T) {
	ctx := context.Background()

	conn, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, err, "open clickhouse")
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 2, 0) {
		// https://clickhouse.com/docs/ru/whats-new/changelog#new-feature-3
		t.Skip(fmt.Errorf("unsupported clickhouse version. JSON not supported in Nullable"))
		return
	}

	tableDDL := func(name string) string {
		return fmt.Sprintf(`
		CREATE TABLE %s
		(
			id Int32,
			payload Nullable(JSON),
			payload2 Nullable(JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)),

		)
		ENGINE = MergeTree
		ORDER BY (id)
	`, name)
	}
	var (
		nilMap  map[string]string
		nilMap2 *clickhouse.JSON

		// responses
		rPayload  map[string]string
		rPayload2 map[string]any
	)

	t.Run("test Nullable(JSON) with typedColumns", func(t *testing.T) {
		// Case 1: passing nil map for both should insert NULL values.
		_ = conn.Exec(ctx, `DROP TABLE IF EXISTS test1_nullable_json`)
		err = conn.Exec(ctx, tableDDL("test1_nullable_json"))
		require.NoError(t, err, "create table")

		batch, err := conn.PrepareBatch(ctx, `INSERT INTO test1_nullable_json (id, payload, payload2) VALUES (?, ?, ?)`)
		require.NoError(t, err, "prepare batch")

		require.NoError(t, batch.Append(1, nilMap, nilMap2))
		require.NoError(t, batch.Send(), "batch send")

		err = conn.QueryRow(ctx, `SELECT payload FROM test1_nullable_json WHERE id = 1`, 2).Scan(&rPayload)
		require.NoError(t, err, "select payload")

		// retrievedPayload should be nil given we inserted NULL value
		require.Nil(t, rPayload, "payload for id=2 should be NULL (batched insert). Got: %#v", rPayload)

		err = conn.QueryRow(ctx, `SELECT payload FROM test1_nullable_json WHERE id = 1`, 3).Scan(&rPayload2)
		require.NoError(t, err, "select payload")

		// retrievedPayload should be nil given we inserted NULL value
		require.Nil(t, rPayload2, "payload for id=2 should be NULL (batched insert). Got: %#v", rPayload2)

	})
	t.Run("test Nullable(JSON) without typedColumns", func(t *testing.T) {
		// Case 2: ignoring any of the JSON field should also insert as NULL values.
		_ = conn.Exec(ctx, `DROP TABLE IF EXISTS test2_nullable_json`)
		err = conn.Exec(ctx, tableDDL("test2_nullable_json"))
		require.NoError(t, err, "create table")

		batch, err := conn.PrepareBatch(ctx, `INSERT INTO test2_nullable_json (id, payload, payload2) VALUES (?, ?, ?)`)
		require.NoError(t, err, "prepare batch")

		batch, err = conn.PrepareBatch(ctx, `INSERT INTO test2_nullable_json (id) VALUES (?)`)
		require.NoError(t, err, "prepare batch")
		require.NoError(t, batch.Append(2)) // ignore both JSON fields
		require.NoError(t, batch.Send(), "batch send")

		err = conn.QueryRow(ctx, `SELECT payload FROM test2_nullable_json WHERE id = 2`, 2).Scan(&rPayload)
		require.NoError(t, err, "select payload")

		// retrievedPayload should be nil given we inserted NULL value
		require.Nil(t, rPayload, "payload for id=2 should be NULL (batched insert). Got: %#v", rPayload)

		err = conn.QueryRow(ctx, `SELECT payload FROM test2_nullable_json WHERE id = 2`, 3).Scan(&rPayload2)
		require.NoError(t, err, "select payload")
		// retrievedPayload should be nil given we inserted NULL value
		require.Nil(t, rPayload2, "payload for id=2 should be NULL (batched insert). Got: %#v", rPayload2)
	})
}
