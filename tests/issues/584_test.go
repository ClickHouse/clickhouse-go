package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestIssue584(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_584"))
	}()

	const ddl = `
	CREATE TABLE issue_584 (
		Col1 Map(String, String)
	) Engine MergeTree() ORDER BY tuple()
	`
	require.NoError(t, conn.Exec(context.Background(), "DROP TABLE IF EXISTS issue_584"))
	require.NoError(t, conn.Exec(context.Background(), ddl))
	require.NoError(t, conn.Exec(context.Background(), "INSERT INTO issue_584 values($1)", map[string]string{
		"key": "value",
	}))
	var event map[string]string
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT * FROM issue_584").Scan(&event))
	assert.Equal(t, map[string]string{
		"key": "value",
	}, event)
}

func TestIssue584Complex(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, conn.Exec(context.Background(), "DROP TABLE issue_584_complex"))
	}()

	const ddl = `
	CREATE TABLE issue_584_complex (
		Col1 Map(String, Map(UInt8, Array(UInt8)))
	) Engine MergeTree() ORDER BY tuple()
	`
	require.NoError(t, conn.Exec(context.Background(), "DROP TABLE IF EXISTS issue_584_complex"))
	require.NoError(t, conn.Exec(context.Background(), ddl))
	col1 := map[string]map[uint8][]uint8{
		"a": {
			100: []uint8{1, 2, 3, 4},
			99:  []uint8{5, 6, 7, 8},
		},
		"d": {
			98: []uint8{10, 11, 12, 13},
		},
	}
	require.NoError(t, conn.Exec(context.Background(), "INSERT INTO issue_584_complex values($1)", col1))
	var event map[string]map[uint8][]uint8
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT * FROM issue_584_complex").Scan(&event))
	assert.Equal(t, col1, event)

}
