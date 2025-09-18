
package issues

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test470PR(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE issue_470_pr (
			Col1 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec("DROP TABLE issue_470_pr")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO issue_470_pr")
	require.NoError(t, err)
	_, err = batch.Exec(nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting <nil> to Array(String) is unsupported")
}
