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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func Test813(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	const ddl = `
		CREATE TABLE test_813 (
		  	IntValue Int64,
			Exemplars Nested (
				Attributes Map(LowCardinality(String), String)
			) CODEC(ZSTD(1)) 
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec("DROP TABLE test_813")
	defer func() {
		conn.Exec("DROP TABLE test_813")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)

	valueArgs := []any{
		int64(14),
		clickhouse.ArraySet{map[string]string{"array1_key1": "array1_value2", "array1_key2": "array1_value2"}},
	}
	_, err = conn.Exec("INSERT INTO test_813 (IntValue, Exemplars.Attributes) VALUES (?,?)", valueArgs...)
	require.NoError(t, err)
}
