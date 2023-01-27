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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdBool(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := GetStdDSNConnection(protocol, useSSL, nil); assert.NoError(t, err) {
				if !CheckMinServerVersion(conn, 21, 12, 0) {
					t.Skip(fmt.Errorf("unsupported clickhouse version"))
					return
				}
				const ddl = `
			CREATE TABLE test_bool (
				    Col1 Bool
				  , Col2 Bool
				  , Col3 Array(Bool)
				  , Col4 Nullable(Bool)
				  , Col5 Array(Nullable(Bool))
			) Engine MergeTree() ORDER BY tuple()
		`
				defer func() {
					conn.Exec("DROP TABLE test_bool")
				}()
				_, err := conn.Exec(ddl)
				require.NoError(t, err)
				scope, err := conn.Begin()
				require.NoError(t, err)
				batch, err := scope.Prepare("INSERT INTO test_bool")
				require.NoError(t, err)
				var val bool
				_, err = batch.Exec(true, false, []bool{true, false, true}, nil, []*bool{&val, nil, &val})
				require.NoError(t, err)
				require.NoError(t, scope.Commit())
				var (
					col1 bool
					col2 bool
					col3 []bool
					col4 *bool
					col5 []*bool
				)
				require.NoError(t, conn.QueryRow("SELECT * FROM test_bool").Scan(&col1, &col2, &col3, &col4, &col5))
				assert.Equal(t, true, col1)
				assert.Equal(t, false, col2)
				assert.Equal(t, []bool{true, false, true}, col3)
				require.Nil(t, col4)
				assert.Equal(t, []*bool{&val, nil, &val}, col5)
			}
		})
	}
}
