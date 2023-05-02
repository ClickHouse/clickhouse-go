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

package std

import (
	"database/sql"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestSimpleStdString(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	connectionString := fmt.Sprintf("http://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60", env.Host, env.HttpPort, env.Username, env.Password)
	if useSSL {
		connectionString = fmt.Sprintf("https://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)
	}
	dsns := map[string]string{"Http": connectionString}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSN(dsn)
			require.NoError(t, err)
			const ddl = `CREATE TABLE test_array (Col1 String, Col2 Nullable(String)) Engine MergeTree() ORDER BY tuple()`
			conn.Exec("DROP TABLE test_array")
			defer func() {
				conn.Exec("DROP TABLE test_array")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_array")
			require.NoError(t, err)
			var (
				col1Data = "A"
			)
			for i := 0; i < 10; i++ {
				_, err := batch.Exec(col1Data, nil)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM test_array")
			require.NoError(t, err)
			for rows.Next() {
				var (
					col1 any
					col2 sql.NullString
				)
				require.NoError(t, rows.Scan(&col1, &col2))
				assert.Equal(t, col1Data, col1)
				assert.Equal(t, sql.NullString{Valid: false}, col2)
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}
