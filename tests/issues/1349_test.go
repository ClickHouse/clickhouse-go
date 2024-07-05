package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1349(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	const ddl = `
		CREATE TABLE test_array (
				Col1 Array(Array(String)),
				Col2 Array(Array(Nullable(String)))
		) Engine MergeTree() ORDER BY tuple()
		`
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP TABLE test_array")

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_array")
	require.NoError(t, err)

	var (
		a        = "a"
		b        = "b"
		col1Data = []interface{}{[]string{}, []string{"a", "b"}, &[]string{"c"}, []interface{}{&a, &b}}
		col2Data = []interface{}{[]*string{&a, nil}, &[]*string{&b, nil}, &[]interface{}{nil, &a}}
	)

	err = batch.Append(col1Data, col2Data)
	require.NoError(t, err)

	err = batch.Send()
	require.NoError(t, err)

	rows, err := conn.Query(ctx, "SELECT * FROM test_array")
	require.NoError(t, err)

	require.True(t, rows.Next())

	var (
		col1 any
		col2 any
	)
	err = rows.Scan(&col1, &col2)
	require.NoError(t, err)

	require.Equal(t, [][]string{{}, {"a", "b"}, {"c"}, {"a", "b"}}, col1)
	require.Equal(t, [][]*string{{&a, nil}, {&b, nil}, {nil, &a}}, col2)

	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}
