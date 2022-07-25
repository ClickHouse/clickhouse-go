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
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestTuple(t *testing.T) {

	var (
		conn = clickhouse.OpenDB(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
		})
	)
	loc, err := time.LoadLocation("Europe/Lisbon")
	require.NoError(t, err)
	localTime := testDate.In(loc)

	if err := checkMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
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
		) Engine Memory
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
		col1Data = []interface{}{"A", int64(42)}
		col2Data = []interface{}{"B", int8(1), localTime.Truncate(time.Second)}
		col3Data = map[string]interface{}{
			"name1": localTime.Truncate(time.Second),
			"name2": "CH",
			"name3": map[string]string{
				"key": "value",
			},
		}
		col4Data = [][][]interface{}{
			[][]interface{}{
				[]interface{}{"Hi", int64(42)},
			},
		}
		col5Data = []interface{}{
			"LCString",
			[]string{"A", "B", "C"},
		}
		str      = "LCString"
		col6Data = []interface{}{
			&str,
			[]*string{&str, nil, &str},
		}
		col7Data = &[]interface{}{"C", int64(42)}
	)
	_, err = batch.Exec(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data)
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 interface{}
		col2 interface{}
		// col3 is a named tuple - we can use map
		col3 interface{}
		col4 interface{}
		col5 interface{}
		col6 interface{}
		col7 interface{}
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM test_tuple").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.NoError(t, err)
	assert.Equal(t, toJson(col1Data), toJson(col1))
	assert.Equal(t, toJson(col2Data), toJson(col2))
	assert.JSONEq(t, toJson(col3Data), toJson(col3))
	assert.Equal(t, toJson(col4Data), toJson(col4))
	assert.Equal(t, toJson(col5Data), toJson(col5))
	assert.Equal(t, toJson(col6Data), toJson(col6))
	assert.Equal(t, toJson(col7Data), toJson(col7))
}
