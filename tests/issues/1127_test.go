package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test1127(t *testing.T) {
	t.Skip("This test is flaky and needs to be fixed")

	var (
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, nil)
	)
	require.NoError(t, err)

	progressHasTriggered := false
	ctx := clickhouse.Context(context.Background(), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
		progressHasTriggered = true
	}), clickhouse.WithLogs(func(log *clickhouse.Log) {
		fmt.Println("log info: ", log)
	}))

	rows, err := conn.Query(ctx, "select number, throwIf(number = 1e6) from system.numbers settings max_block_size = 100")
	require.NoError(t, err)
	defer rows.Close()

	var number uint64
	var throwIf uint8
	for rows.Next() {
		require.NoError(t, rows.Scan(&number, &throwIf))
	}

	assert.Error(t, rows.Err())
	assert.True(t, progressHasTriggered)
}
