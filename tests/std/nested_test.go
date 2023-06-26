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
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestStdNested(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}
	conn, err := GetStdOpenDBConnection(clickhouse.Native, clickhouse.Settings{
		"flatten_nested": 0,
	}, tlsConfig, nil)
	require.NoError(t, err)
	conn.Exec("DROP TABLE std_nested_test")
	if !CheckMinServerVersion(conn, 22, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
			CREATE TABLE std_nested_test (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine MergeTree() ORDER BY tuple()`
	defer func() {
		conn.Exec("DROP TABLE std_nested_test")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO std_nested_test")
	require.NoError(t, err)
	var (
		col1Data = []map[string]any{
			{
				"Col1_N1": uint8(1),
				"Col2_N1": uint8(20),
			},
			{
				"Col1_N1": uint8(2),
				"Col2_N1": uint8(20),
			},
			{
				"Col1_N1": uint8(3),
				"Col2_N1": uint8(20),
			},
		}
		col2Data = []map[string]any{
			{
				"Col1_N2": uint8(101),
				"Col2_N2": []map[string]any{
					{
						"Col1_N2_N1": uint8(1),
						"Col2_N2_N1": uint8(2),
					},
				},
			},
			{
				"Col1_N2": uint8(201),
				"Col2_N2": []map[string]any{
					{
						"Col1_N2_N1": uint8(3),
						"Col2_N2_N1": uint8(4),
					},
				},
			},
		}
	)

	_, err = batch.Exec(col1Data, col2Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 []map[string]any
		col2 []map[string]any
	)
	rows := conn.QueryRow("SELECT * FROM std_nested_test")
	require.NoError(t, rows.Scan(&col1, &col2))
	assert.JSONEq(t, ToJson(col1Data), ToJson(col1))
	assert.JSONEq(t, ToJson(col2Data), ToJson(col2))
}
