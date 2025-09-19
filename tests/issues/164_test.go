
package issues

import (
	"context"
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestIssue164(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE issue_164 (
			  Col1 Int32
			, Col2 Array(Int8)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec("DROP TABLE issue_164")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO issue_164")
	require.NoError(t, err)
	stmtParams := make([]any, 0)
	stmtParams = append(stmtParams, sql.NamedArg{Name: "id", Value: int32(10)})
	stmtParams = append(stmtParams, sql.NamedArg{Name: "anything", Value: nil})
	_, err = batch.ExecContext(context.Background(), stmtParams...)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "converting <nil> to Array(Int8) is unsupported")
}
