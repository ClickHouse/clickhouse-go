package tests

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestDurationInt64(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time": 60,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	require.NoError(t, conn.Exec(
		context.Background(),
		`
			CREATE TABLE IF NOT EXISTS issue_631
			(timeDuration Int64)
			ENGINE = MergeTree
			ORDER BY (timeDuration)
			`,
	))
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_631"))
	}()

	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO issue_631 (timeDuration)")
	require.NoError(t, err)
	require.NoError(t, batch.Append(time.Duration(time.Second)*120))
	require.NoError(t, batch.Send())
	row := conn.QueryRow(context.Background(), "SELECT timeDuration from issue_631")
	require.NoError(t, err)
	var timeDuration time.Duration
	require.NoError(t, row.Scan(&timeDuration))
	assert.Equal(t, time.Duration(time.Second)*120, timeDuration)
}
