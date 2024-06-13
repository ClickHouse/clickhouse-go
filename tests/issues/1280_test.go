package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1280(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection(testSet, clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": true,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)

	ddl := "CREATE TABLE values (`id` Int32, `values` Int32) Engine = Memory"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS values")

	testCases1 := []struct {
		input string
	}{
		{
			input: "INSERT INTO values (values)",
		},
		{
			input: "INSERT INTO values (values) values",
		},
		{
			input: "INSERT INTO values (`values`) values",
		},
		{
			input: "INSERT INTO values(values)",
		},
	}

	for i, tc := range testCases1 {
		batch, err := conn.PrepareBatch(context.Background(), tc.input)
		require.NoError(t, err)
		appendErr := batch.Append(i)
		require.NoError(t, appendErr)
		err = batch.Send()
		require.NoError(t, err)
	}

	testCases2 := []struct {
		input string
	}{
		{
			input: `
				INSERT
				INTO
				values
				(
					id,
					values
				)`,
		},
		{
			input: `INSERT 
					INTO
					values
					(id,
						values)
					values`,
		},
		{
			input: `
					INSERT
					 INTO 
					 values
					  (id,values) values (1,2)`,
		},
		{
			input: `INSERT INTO values(id, values) values (1,2)`,
		},
	}

	for i, tc := range testCases2 {
		batch, err := conn.PrepareBatch(context.Background(), tc.input)
		require.NoError(t, err)
		appendErr := batch.Append(i, i)
		require.NoError(t, appendErr)
		err = batch.Send()
		require.NoError(t, err)
	}
}
