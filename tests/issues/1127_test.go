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
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": true,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)

	progressHasTriggered := false
	ctx := clickhouse.Context(context.Background(), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
		progressHasTriggered = true
	}), clickhouse.WithLogs(func(log *clickhouse.Log) {
		fmt.Println("log info: ", log)
	}))

	rows, err := conn.Query(ctx, "select throwIf(number = 1e6) from system.numbers settings max_block_size = 100")
	require.NoError(t, err)

	defer rows.Close()
	for rows.Next() {
	}

	assert.Error(t, rows.Err())
	assert.True(t, progressHasTriggered)
}
