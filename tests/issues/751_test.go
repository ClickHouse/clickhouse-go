package issues

import (
	"context"
	"database/sql"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue751(t *testing.T) {
	conn, err := clickhouse_tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	ctx := context.Background()
	conn.Exec(ctx, "DROP TABLE IF EXISTS issue_751")

	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE issue_751 (
				Col1 Nullable(String),
				Col2 String,
				Col3 Nullable(Int8),
				Col4 Nullable(Int64),
				Col5 LowCardinality(Nullable(String)),
			)
			Engine Memory
		`))
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_751")
	}()
	type Example struct {
		Col1 *string
		Col2 string
		Col3 *int8
		Col4 *int64
		Col5 *string
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_751")
	require.NoError(t, err)
	require.NoError(t, batch.AppendStruct(&Example{}))

	require.NoError(t, batch.Send())

	var (
		col1 *string
		col2 string
		col3 *int8
		col4 sql.NullInt64
		col5 *string
	)

	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM issue_751").Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Nil(t, col1)
	assert.Equal(t, "", col2)
	assert.Nil(t, col3)
	assert.Equal(t, sql.NullInt64{
		Int64: 0,
		Valid: false,
	}, col4)
	assert.Nil(t, col5)
}
