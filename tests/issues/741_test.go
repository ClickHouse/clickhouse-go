package issues

import (
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"strings"
	"testing"
)

func TestIssue741(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, "false")
	require.NoError(t, err)
	conn.Exec("DROP TABLE IF EXISTS issue_741")
	ddl := `
		CREATE TABLE issue_741 (
				Col1 String,
				Col2 Int64
			)
			Engine MergeTree() ORDER BY tuple()
		`
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	defer func() {
		conn.Exec("DROP TABLE issue_741")
	}()
	stmt, err := conn.Prepare("INSERT INTO issue_741 (Col2, Col1) VALUES (? ?)")
	_, err = stmt.Exec(int64(1), "1")
	require.NoError(t, err)
}

func TestIssue741SingleColumn(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, "false")
	require.NoError(t, err)
	conn.Exec("DROP TABLE IF EXISTS issue_741_single")
	ddl := `
		CREATE TABLE issue_741_single (
				Col1 String,
				Col2 Int64
			)
			Engine MergeTree() ORDER BY tuple()
		`
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	defer func() {
		conn.Exec("DROP TABLE issue_741_single")
	}()
	stmt, err := conn.Prepare("INSERT INTO issue_741_single (Col1) VALUES (?)")
	_, err = stmt.Exec("1")
	require.NoError(t, err)
}

func TestIssue741RandomOrder(t *testing.T) {
	columns := map[string]interface{}{
		"Col1 String": "a",
		"Col2 Int64":  int64(1),
		"Col3 Int32":  int32(2),
		"Col4 Bool":   true,
	}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, "false")
	require.NoError(t, err)
	conn.Exec("DROP TABLE IF EXISTS issue_741_random")
	colNames := make([]string, len(columns))
	i := 0
	for k := range columns {
		colNames[i] = k
		i++
	}
	// shuffle our columns for ddl
	rand.Shuffle(len(colNames), func(i, j int) { colNames[i], colNames[j] = colNames[j], colNames[i] })
	ddl := fmt.Sprintf(`
			CREATE TABLE issue_741_random (
				%s
			)
			Engine MergeTree() ORDER BY tuple()`, strings.Join(colNames, ", "))
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	defer func() {
		conn.Exec("DROP TABLE issue_741_random")
	}()
	// shuffle our columns for insert
	rand.Shuffle(len(colNames), func(i, j int) { colNames[i], colNames[j] = colNames[j], colNames[i] })
	names := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	for i := range colNames {
		names[i] = strings.Fields(colNames[i])[0]
		placeholders[i] = "?"
	}
	stmt, err := conn.Prepare(fmt.Sprintf("INSERT INTO issue_741_random (%s) VALUES (%s)", strings.Join(names, ", "), strings.Join(placeholders, ", ")))
	require.NoError(t, err)
	values := make([]interface{}, len(colNames))
	for i, colName := range colNames {
		values[i] = columns[colName]
	}
	_, err = stmt.Exec(values...)
	require.NoError(t, err)
}

// test Append
