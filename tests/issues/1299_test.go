package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue1299(t *testing.T) {
	ctx := context.Background()
	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	expectedEnumValue := "raw:48h',1h:63d,1d:5y"

	const ddl = `
		CREATE TABLE test_1299 (
				Col1 Enum ('raw:48h\',1h:63d,1d:5y' = 1, 'raw:8h,1m:48h,1h:63d,1d:5y' = 2)
		) Engine MergeTree() ORDER BY tuple()
		`
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP TABLE test_1299")

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1299")
	require.NoError(t, err)
	require.NoError(t, batch.Append(expectedEnumValue))
	require.NoError(t, batch.Send())

	var actualEnumValue string
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_1299").Scan(&actualEnumValue))

	assert.Equal(t, expectedEnumValue, actualEnumValue)
}
