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

func TestStdEnum(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
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

func TestStdEnumInsertAsInt(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			const ddl = `
			CREATE TABLE test_enum_int (
				  Col1 Enum  ('hello' = 1,  'world' = 2)
				, Col2 Enum8 ('click' = 5,  'house' = 25)
				, Col3 Enum16('house' = 10,   'value' = 50)
				, Col4 Array(Enum8  ('click' = 1, 'house' = 2))
				, Col5 Array(Enum16 ('click' = 1, 'house' = 2))
				, Col6 Array(Nullable(Enum8  ('click' = 1, 'house' = 2)))
				, Col7 Array(Nullable(Enum16 ('click' = 1, 'house' = 2)))
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_enum_int")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_enum_int")
			require.NoError(t, err)
			var (
				col6Int8Val  = int8(2)
				col7Int16Val = int16(2)
				col1Data     = 1
				col2Data     = int8(5)
				col3Data     = int16(10)
				col4Data     = []int8{1, 2}
				col5Data     = []int16{2, 1}
				col6Data     = []*int8{&col6Int8Val, nil, &col6Int8Val}
				col7Data     = []*int16{&col7Int16Val, nil, &col7Int16Val}
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
				col1 int8
				col2 int8
				col3 int16
				col4 []string
				col5 []string
				col6 []*string
				col7 []*string
			)
			require.NoError(t, conn.QueryRow(`SELECT 
					  CAST(Col1, 'Int8') AS Col1
					, CAST(Col2, 'Int8') AS Col2
					, CAST(Col3, 'Int16') AS Col3
					, Col4
					, Col5
					, Col6
					, Col7
					FROM test_enum_int`).Scan(
				&col1, &col2, &col3, &col4,
				&col5, &col6, &col7,
			))
			assert.Equal(t, int8(col1Data), col1) // cast for comparing correctly
			assert.Equal(t, col2Data, col2)
			assert.Equal(t, col3Data, col3)
			assert.Equal(t, []string{"click", "house"}, col4)
			assert.Equal(t, []string{"house", "click"}, col5)
			houseVal := "house"
			assert.Equal(t, []*string{&houseVal, nil, &houseVal}, col6)
			assert.Equal(t, []*string{&houseVal, nil, &houseVal}, col7)

			// Error should be thrown for invalid Enum value
			scopeFail, err := conn.Begin()
			require.NoError(t, err)
			batchFail, err := scopeFail.Prepare("INSERT INTO test_enum_int")
			require.NoError(t, err)
			_, err = batchFail.Exec(
				col1Data,
				col2Data,
				100, // Only 10,50 are known
				col4Data,
				col5Data,
				col6Data,
				col7Data,
			)
			require.Error(t, err)
			require.Error(t, scope.Rollback()) // Errors because already rolled-back
		})
	}
}
