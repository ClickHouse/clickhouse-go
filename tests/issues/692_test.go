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
	"fmt"
	"net/url"
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/ClickHouse/clickhouse-go/v2/tests/std"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue692(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, url.Values{})
	require.NoError(t, err)
	if !std.CheckMinServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_map (
			  Col1 Map(String, UInt64)
			, Col2 Map(String, UInt64)
			, Col3 Map(String, UInt64)
			, Col4 Array(Map(String, String))
			, Col5 Map(LowCardinality(String), LowCardinality(String))
			, Col6 Map(String, Map(String, Int64))
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec("DROP TABLE test_map")
	defer func() {
		conn.Exec("DROP TABLE test_map")
	}()

	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO test_map")
	require.NoError(t, err)
	var (
		col1Data = map[string]uint64{
			"key_col_1_1": 1,
			"key_col_1_2": 2,
		}
		col2Data = map[string]uint64{
			"key_col_2_1": 10,
			"key_col_2_2": 20,
		}
		col3Data = map[string]uint64{}
		col4Data = []map[string]string{
			{"A": "B"},
			{"C": "D"},
		}
		col5Data = map[string]string{
			"key_col_5_1": "100",
			"key_col_5_2": "200",
		}
		col6Data = map[string]map[string]int64{}
	)
	_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 any
		col2 map[string]uint64
		col3 map[string]uint64
		col4 []map[string]string
		col5 map[string]string
		col6 map[string]map[string]int64
	)
	require.NoError(t, conn.QueryRow("SELECT Col1, Col2, Col3, Col4, Col5, Col6 FROM test_map").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
}
