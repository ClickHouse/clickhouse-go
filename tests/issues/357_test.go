package issues

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssue357(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)

	const ddl = ` -- foo.bar DDL comment
		CREATE TEMPORARY TABLE issue_357 (
			  Col1 Int32
			, Col2 DateTime
		)
		`
	defer func() {
		conn.Exec("DROP TABLE issue_357")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	const query = ` -- foo.bar Insert comment
				INSERT INTO issue_357
				`
	batch, err := scope.Prepare(query)

	require.NoError(t, err)
	_, err = batch.Exec(int32(42), time.Now())
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 int32
		col2 time.Time
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM issue_357").Scan(&col1, &col2))
	assert.Equal(t, int32(42), col1)
}
