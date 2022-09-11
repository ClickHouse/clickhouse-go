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
	"github.com/rnbondarenko/clickhouse-go/v2"
	clickhouse_tests "github.com/rnbondarenko/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdEnum(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, "false")
			require.NoError(t, err)
			const ddl = `
			CREATE TABLE test_enum (
				  Col1 Enum  ('hello'   = 1,  'world' = 2)
				, Col2 Enum8 ('click'   = 5,  'house' = 25)
				, Col3 Enum16('house' = 10,   'value' = 50)
				, Col4 Array(Enum8  ('click' = 1, 'house' = 2))
				, Col5 Array(Enum16 ('click' = 1, 'house' = 2))
				, Col6 Array(Nullable(Enum8  ('click' = 1, 'house' = 2)))
				, Col7 Array(Nullable(Enum16 ('click' = 1, 'house' = 2)))
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_enum")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_enum")
			require.NoError(t, err)
			var (
				col1Data = "hello"
				col2Data = "click"
				col3Data = "house"
				col4Data = []string{"click", "house"}
				col5Data = []string{"house", "click"}
				col6Data = []*string{&col2Data, nil, &col3Data}
				col7Data = []*string{&col3Data, nil, &col2Data}
			)
			_, err = batch.Exec(
				col1Data,
				col2Data,
				col3Data,
				col4Data,
				col5Data,
				col6Data,
				col7Data,
			)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 string
				col2 string
				col3 string
				col4 []string
				col5 []string
				col6 []*string
				col7 []*string
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_enum").Scan(
				&col1, &col2, &col3, &col4,
				&col5, &col6, &col7,
			))
			assert.Equal(t, col1Data, col1)
			assert.Equal(t, col2Data, col2)
			assert.Equal(t, col3Data, col3)
			assert.Equal(t, col4Data, col4)
			assert.Equal(t, col5Data, col5)
			assert.Equal(t, col6Data, col6)
			assert.Equal(t, col7Data, col7)
		})
	}
}
