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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSimpleStdString(t *testing.T) {
	dsns := map[string]string{"Http": "http://127.0.0.1:8123,127.0.0.1:8123/default?dial_timeout=200ms&max_execution_time=60"}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				const ddl = `
		CREATE TABLE test_array (
			  Col1 String
		) Engine Memory
		`
				defer func() {
					conn.Exec("DROP TABLE test_array")
				}()
				_, err := conn.Exec(ddl)
				require.NoError(t, err)
				scope, err := conn.Begin()
				require.NoError(t, err)
				batch, err := scope.Prepare("INSERT INTO test_array")
				require.NoError(t, err)
				var (
					col1Data = "A"
				)
				for i := 0; i < 10; i++ {
					_, err := batch.Exec(col1Data)
					require.NoError(t, err)
				}
				require.NoError(t, scope.Commit())
				rows, err := conn.Query("SELECT * FROM test_array")
				require.NoError(t, err)
				for rows.Next() {
					var (
						col1 interface{}
					)
					require.NoError(t, rows.Scan(&col1))
					assert.Equal(t, col1Data, col1)
				}
				require.NoError(t, rows.Close())
				require.NoError(t, rows.Err())
			}
		})
	}
}
