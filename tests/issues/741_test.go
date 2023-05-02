// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package issues

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestIssue741(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	for _, protocol := range protocols {
		t.Run(fmt.Sprintf("%v Protocol", protocol), func(t *testing.T) {
			conn, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
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
		})
	}
}

func TestIssue741SingleColumn(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	for _, protocol := range protocols {
		t.Run(fmt.Sprintf("%v Protocol", protocol), func(t *testing.T) {
			conn, err := clickhouse_std_tests.GetDSNConnection("issues", protocol, useSSL, nil)
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
		})
	}
}

func generateRandomInsert(tableName string) (string, string, []any) {
	columns := map[string]any{
		"Col1 String":       "a",
		"Col2 Int64":        int64(1),
		"Col3 Int32":        int32(2),
		"Col4 Bool":         true,
		"Col5 Date32":       time.Now(),
		"Col6 IPv4":         net.ParseIP("8.8.8.8"),
		"Col7 Decimal32(5)": decimal.New(25, 0),
		"Col8 UUID":         uuid.New(),
	}
	colNames := make([]string, len(columns))
	i := 0
	for k := range columns {
		colNames[i] = k
		i++
	}
	// shuffle our columns for ddl
	rand.Shuffle(len(colNames), func(i, j int) { colNames[i], colNames[j] = colNames[j], colNames[i] })
	ddl := fmt.Sprintf(`
			CREATE TABLE %s (
				%s
			)
			Engine MergeTree() ORDER BY tuple()`, tableName, strings.Join(colNames, ", "))
	// shuffle our columns for insert
	rand.Shuffle(len(colNames), func(i, j int) { colNames[i], colNames[j] = colNames[j], colNames[i] })
	names := make([]string, len(colNames))
	placeholders := make([]string, len(colNames))
	for i := range colNames {
		names[i] = strings.Fields(colNames[i])[0]
		placeholders[i] = "?"
	}
	insertStatement := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(names, ", "), strings.Join(placeholders, ", "))
	values := make([]any, len(colNames))
	for i, colName := range colNames {
		values[i] = columns[colName]
	}
	return ddl, insertStatement, values
}

func TestIssue741RandomOrder(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	for _, protocol := range protocols {
		t.Run(fmt.Sprintf("%v Protocol", protocol), func(t *testing.T) {
			conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS issue_741_random")
			defer func() {
				conn.Exec("DROP TABLE issue_741_random")
			}()
			ddl, insertStatement, values := generateRandomInsert("issue_741_random")
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			stmt, err := conn.Prepare(fmt.Sprintf(insertStatement))
			require.NoError(t, err)
			_, err = stmt.Exec(values...)
			require.NoError(t, err)
		})
	}
}

// test Append on native connection
func TestIssue741NativeAppend(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	conn.Exec(ctx, "DROP TABLE IF EXISTS issue_741_append_random")
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_741_append_random")
	}()
	ddl, insertStatement, values := generateRandomInsert("issue_741_append_random")
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, insertStatement)
	require.NoError(t, err)
	require.NoError(t, batch.Append(values...))
	require.NoError(t, batch.Send())
}

// test Append on native connection
func TestIssue741StdAppend(t *testing.T) {
	//test http and native
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	protocols := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	for _, protocol := range protocols {
		t.Run(fmt.Sprintf("%v Protocol", protocol), func(t *testing.T) {
			conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS issue_741_std_append_random")
			defer func() {
				conn.Exec("DROP TABLE issue_741_std_append_random")
			}()
			ddl, insertStatement, values := generateRandomInsert("issue_741_std_append_random")
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare(insertStatement)
			require.NoError(t, err)
			_, err = batch.Exec(values...)
			require.NoError(t, err)
		})
	}
}
