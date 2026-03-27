package issues

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
)

func TestIssue389(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 20, 3, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TEMPORARY TABLE issue_389 (
				    Col1 DateTime64(3, 'America/New_York')
			)
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_389")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_389")
	require.NoError(t, err)
	require.NoError(t, batch.Append(int64(1625128291293)))
	require.NoError(t, batch.Send())
	var col1 time.Time
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM issue_389").Scan(&col1))
	require.Equal(t, "America/New_York", col1.Location().String())
	assert.Equal(t, "2021-07-01 04:31:31.293 -0400 EDT", col1.String())
}
