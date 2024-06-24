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
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdLowCardinality(t *testing.T) {
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{}))
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 19, 11, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
		CREATE TABLE test_lowcardinality (
		      ID UInt64
			, Col1 LowCardinality(String)
			, Col2 LowCardinality(FixedString(2))
			, Col3 Array(LowCardinality(String))
			, Col4 Array(Array(LowCardinality(String)))
			, Col5 LowCardinality(Nullable(String))
			, Col6 Array(Array(LowCardinality(Nullable(String))))
		) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_lowcardinality")
			}()
			_, err = conn.ExecContext(ctx, ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_lowcardinality")
			require.NoError(t, err)
			var (
				timestamp = time.Now()
			)
			for i := 0; i < 10; i++ {
				var (
					id       = uint64(i)
					col1Data = timestamp.String()
					col2Data = "RU"
					col3Data = []string{"A", "B", "C"}
					col4Data = [][]string{
						[]string{"Q", "W", "E"},
						[]string{"R", "T", "Y"},
					}
					col5Data = &col2Data
					col6Data = [][]*string{
						[]*string{&col2Data, nil, &col2Data},
						[]*string{nil, &col2Data, nil},
					}
				)
				if i%2 == 0 {
					if _, err := batch.Exec(id, col1Data, col2Data, col3Data, col4Data, col5Data, col6Data); !assert.NoError(t, err) {
						return
					}
				} else {
					if _, err := batch.Exec(id, col1Data, col2Data, col3Data, col4Data, nil, col6Data); !assert.NoError(t, err) {
						return
					}
				}
			}
			require.NoError(t, scope.Commit())
			var count uint64
			require.NoError(t, conn.QueryRow("SELECT COUNT() FROM test_lowcardinality").Scan(&count))
			assert.Equal(t, uint64(10), count)

			for i := 0; i < 10; i++ {
				var (
					id   uint64
					col1 string
					col2 string
					col3 []string
					col4 [][]string
					col5 *string
					col6 [][]*string
				)
				require.NoError(t, conn.QueryRow("SELECT * FROM test_lowcardinality WHERE ID = $1", i).Scan(&id, &col1, &col2, &col3, &col4, &col5, &col6))
				assert.Equal(t, timestamp.String(), col1)
				assert.Equal(t, "RU", col2)
				assert.Equal(t, []string{"A", "B", "C"}, col3)
				assert.Equal(t, [][]string{
					[]string{"Q", "W", "E"},
					[]string{"R", "T", "Y"},
				}, col4)
				switch {
				case i%2 == 0:
					assert.Equal(t, &col2, col5)
				default:
					assert.Nil(t, col5)
				}
				col2Data := "RU"
				assert.Equal(t, [][]*string{
					[]*string{&col2Data, nil, &col2Data},
					[]*string{nil, &col2Data, nil},
				}, col6)
			}
		})
	}
}
