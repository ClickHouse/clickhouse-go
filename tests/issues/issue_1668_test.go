package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1668 verifies that nil JSON values can be inserted when the JSON type
// has Array and Tuple typed paths: a NULL for Nullable(JSON), and an empty
// array and a zero-valued tuple for the typed paths of a non-nullable JSON.
func Test1668(t *testing.T) {
	ctx := context.Background()

	conn, err := clickhouse_tests.GetConnectionTCP("issues", nil, nil, nil)
	require.NoError(t, err)
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 25, 2, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version. JSON not supported in Nullable"))
		return
	}

	const ddl = `
		CREATE TABLE test_1668
		(
			id Int32,
			payload Nullable(JSON(a Array(Int64), t Tuple(x Int64, y String))),
			payload2 JSON(a Array(Int64), t Tuple(x Int64, y String))
		)
		ENGINE = MergeTree
		ORDER BY (id)
	`
	require.NoError(t, conn.Exec(ctx, `DROP TABLE IF EXISTS test_1668`))
	require.NoError(t, conn.Exec(ctx, ddl))
	t.Cleanup(func() {
		_ = conn.Exec(ctx, `DROP TABLE IF EXISTS test_1668`)
	})

	var (
		nilMap  map[string]any
		nilJSON *clickhouse.JSON
	)
	value := map[string]any{
		"a": []int64{1, 2},
		"t": []any{int64(3), "foo"},
	}

	batch, err := conn.PrepareBatch(ctx, `INSERT INTO test_1668 (id, payload, payload2) VALUES (?, ?, ?)`)
	require.NoError(t, err)
	require.NoError(t, batch.Append(1, nilMap, nilJSON))
	require.NoError(t, batch.Append(2, value, value))
	require.NoError(t, batch.Append(3, nilJSON, nilMap))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, `SELECT id, payload IS NULL, toString(payload2.a), toString(payload2.t) FROM test_1668 ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	type result struct {
		id     int32
		isNull bool
		a      string
		t      string
	}
	expected := []result{
		{id: 1, isNull: true, a: "[]", t: "(0,'')"},
		{id: 2, isNull: false, a: "[1,2]", t: "(3,'foo')"},
		{id: 3, isNull: true, a: "[]", t: "(0,'')"},
	}
	var actual []result
	for rows.Next() {
		var r result
		require.NoError(t, rows.Scan(&r.id, &r.isNull, &r.a, &r.t))
		actual = append(actual, r)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, expected, actual)
}
