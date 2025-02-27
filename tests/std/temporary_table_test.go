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
	"context"
	"fmt"
	"strconv"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdTemporaryTable(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			ctx := context.Background()
			if name == "Http" {
				ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
					"session_id": "temp_table_test_session",
				}))
			}

			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			_, err = conn.Exec("DROP TABLE IF EXISTS test_temporary_table")
			require.NoError(t, err)
			const ddl = `CREATE TEMPORARY TABLE test_temporary_table (
							ID UInt64
						);`
			tx, err := conn.Begin()
			require.NoError(t, err)
			conn.ExecContext(ctx, "DROP TABLE IF EXISTS test_temporary_table")
			_, err = tx.ExecContext(ctx, ddl)
			require.NoError(t, err)
			_, err = tx.ExecContext(ctx, "INSERT INTO test_temporary_table (ID) SELECT number AS ID FROM system.numbers LIMIT 10")
			require.NoError(t, err)
			rows, err := tx.QueryContext(ctx, "SELECT ID AS ID FROM test_temporary_table")
			require.NoError(t, err)
			var count int
			for rows.Next() {
				var num int
				if err := rows.Scan(&num); !assert.NoError(t, err) {
					return
				}
				count++
			}
			_, err = tx.QueryContext(ctx, "SELECT ID AS ID1 FROM test_temporary_table")
			require.NoError(t, err)
			_, err = conn.Query("SELECT ID AS ID2 FROM test_temporary_table")
			require.Error(t, err)
			if name == "Http" {
				assert.Contains(t, err.Error(), "Code: 60")
			} else {
				exception, ok := err.(*clickhouse.Exception)
				require.True(t, ok)
				assert.Equal(t, int32(60), exception.Code)
			}
			require.Equal(t, 10, count)
			require.NoError(t, tx.Commit())

			_, err = conn.Exec("DROP TABLE IF EXISTS test_temporary_table")
			require.NoError(t, err)
			require.NoError(t, conn.Close())
		})
	}
}
