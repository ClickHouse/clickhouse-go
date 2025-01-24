package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1468(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	const ddl = `
		CREATE TABLE IF NOT EXISTS issue_1468(
			some_id    UInt64,
			some_title String
		) ENGINE = MergeTree PRIMARY KEY (some_id) ORDER BY (some_id)
	`
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP TABLE issue_1468")

	err = conn.Exec(ctx, "ALTER TABLE issue_1468 DROP COLUMN wrong")
	require.Error(t, err)

	err = conn.Exec(ctx, "ALTER TABLE issue_1468 DROP COLUMN some_title")
	require.NoError(t, err)
}
