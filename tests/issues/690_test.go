package issues

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test690(t *testing.T) {
	conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000")
	require.NoError(t, err)
	if err := std.CheckMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
		CREATE TABLE test_date (
			Id Int64,
			Col3 DateTime64(3),
		    Col4 DateTime64(3, 'UTC')
		) Engine Memory
		`
	conn.Exec("DROP TABLE test_date")
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO test_date")
	require.NoError(t, err)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	tv, err := time.Parse("2006-01-02 15:04:05.999", "2022-07-20 17:42:48.129")
	at := tv.In(loc)
	_, err = batch.Exec(
		int64(23),
		at,
		at,
	)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
}
