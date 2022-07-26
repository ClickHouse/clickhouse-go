package issues

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test693(t *testing.T) {
	conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000")
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_date (
				  ID   UInt8
				, Col1 Date
			) Engine Memory
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
	_, err = batch.Exec(uint8(2), date)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		result1 result
		result2 result
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM test_date WHERE ID = $1", 1).Scan(
		&result1.ColID,
		&result1.Col1,
	))
	require.Equal(t, date, result1.Col1)
	assert.Equal(t, "UTC", result1.Col1.Location().String())
	require.NoError(t, conn.QueryRow("SELECT * FROM test_date WHERE ID = $1", 2).Scan(
		&result2.ColID,
		&result2.Col1,
	))
	require.Equal(t, date, result2.Col1)
	assert.Equal(t, "UTC", result2.Col1.Location().String())
}
