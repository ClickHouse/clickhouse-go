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
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdIPv4(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			const ddl = `
			CREATE TABLE test_ipv4 (
				  Col1 IPv4
				, Col2 IPv4
				, Col3 Nullable(IPv4)
				, Col4 Array(IPv4)
				, Col5 Array(Nullable(IPv4))
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_ipv4")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_ipv4")
			require.NoError(t, err)
			var (
				col1Data = net.ParseIP("127.0.0.1")
				col2Data = net.ParseIP("8.8.8.8")
				col3Data = col1Data
				col4Data = []net.IP{col1Data, col2Data}
				col5Data = []*net.IP{&col1Data, nil, &col2Data}
			)
			_, err = batch.Exec(col1Data, col2Data, &col3Data, &col4Data, &col5Data)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 net.IP
				col2 net.IP
				col3 *net.IP
				col4 []net.IP
				col5 []*net.IP
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_ipv4").Scan(&col1, &col2, &col3, &col4, &col5))
			assert.Equal(t, col1Data.To4(), col1)
			assert.Equal(t, col2Data.To4(), col2)
			assert.Equal(t, col3Data.To4(), *col3)
			require.Len(t, col4, 2)
			assert.Equal(t, col1Data.To4(), col4[0])
			assert.Equal(t, col2Data.To4(), col4[1])
			require.Len(t, col5, 3)
			require.Nil(t, col5[1])
			assert.Equal(t, col1Data.To4(), *col5[0])
			assert.Equal(t, col2Data.To4(), *col5[2])
		})
	}
}
