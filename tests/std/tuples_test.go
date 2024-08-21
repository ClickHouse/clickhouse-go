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
	"strconv"
	"testing"
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

var testDate, _ = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2022-05-25 17:20:57 +0100 WEST")

func TestTuple(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, tlsConfig, nil)
	require.NoError(t, err)
	loc, err := time.LoadLocation("Europe/Lisbon")
	require.NoError(t, err)
	localTime := testDate.In(loc)

	if !CheckMinServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_tuple (
			  Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime('Europe/Lisbon'))
			, Col3 Tuple(name1 DateTime('Europe/Lisbon'), name2 FixedString(2), name3 Map(String, String))
			, Col4 Array(Array( Tuple(String, Int64) ))
			, Col5 Tuple(LowCardinality(String),           Array(LowCardinality(String)))
			, Col6 Tuple(LowCardinality(Nullable(String)), Array(LowCardinality(Nullable(String))))
			, Col7 Tuple(String, Int64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec("DROP TABLE test_tuple")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	batch, err := scope.Prepare("INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = []any{"A", int64(42)}
		col2Data = []any{"B", int8(1), localTime.Truncate(time.Second)}
		col3Data = map[string]any{
			"name1": localTime.Truncate(time.Second),
			"name2": "CH",
			"name3": map[string]string{
				"key": "value",
			},
		}
		col4Data = [][][]any{
			[][]any{
				[]any{"Hi", int64(42)},
			},
		}
		col5Data = []any{
			"LCString",
			[]string{"A", "B", "C"},
		}
		str      = "LCString"
		col6Data = []any{
			&str,
			[]*string{&str, nil, &str},
		}
		col7Data = &[]any{"C", int64(42)}
	)
	_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 any
		col2 any
		// col3 is a named tuple - we can use map
		col3 any
		col4 any
		col5 any
		col6 any
		col7 any
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM test_tuple").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.NoError(t, err)
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, *col7Data, col7)
}
