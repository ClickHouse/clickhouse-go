package issues

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func TestIssue693(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_date (
				  ID   UInt8
				, Col1 Date
			) Engine MergeTree() ORDER BY tuple()
		`
	type result struct {
		ColID uint8 `ch:"ID"`
		Col1  time.Time
	}
	conn.Exec("DROP TABLE test_date")
	defer func() {
		conn.Exec("DROP TABLE test_date")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO test_date")
	require.NoError(t, err)
	// date, err := time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	CurrentLoc, _ := time.LoadLocation("Asia/Shanghai")
	date, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-12 00:00:00", CurrentLoc)
	require.NoError(t, err)
	_, err = batch.Exec(uint8(1), date)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		result1 result
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM test_date WHERE ID = $1", 1).Scan(
		&result1.ColID,
		&result1.Col1,
	))
	require.Equal(t, date.Format("2006-01-02"), result1.Col1.Format("2006-01-02"))
	assert.Equal(t, "UTC", result1.Col1.Location().String())
}
