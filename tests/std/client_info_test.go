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
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"runtime"
	"strconv"
	"testing"
)

func TestClientInfo(t *testing.T) {
	expectedClientProduct := fmt.Sprintf(
		"%s/%d.%d.%d (database/sql; lv:go/%s; os:%s)",
		clickhouse.ClientName,
		clickhouse.ClientVersionMajor,
		clickhouse.ClientVersionMinor,
		clickhouse.ClientVersionPatch,
		runtime.Version()[2:],
		runtime.GOOS,
	)

	testCases := map[string]struct {
		expectedClientInfo string
		additionalOpts     url.Values
	}{
		"no additional products": {
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			expectedClientProduct,
			nil,
		},
		"one additional product": {
			// e.g. tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("tests/dev %s", expectedClientProduct),
			url.Values{
				"client_info_product": []string{"tests/dev"},
			},
		},
		"two additional products": {
			// e.g. product/version tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("product/version tests/dev %s", expectedClientProduct),
			url.Values{
				"client_info_product": []string{"product/version,tests/dev"},
			},
		},
	}

	dsns := []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for _, protocol := range dsns {
		t.Run(fmt.Sprintf("%s protocol", protocol.String()), func(t *testing.T) {
			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					conn, err := GetStdDSNConnection(protocol, useSSL, testCase.additionalOpts)
					require.NoError(t, err)

					actualClientInfo := getConnectedClientInfo(t, conn)
					assert.Equal(t, testCase.expectedClientInfo, actualClientInfo)
				})
			}
		})
	}
}

func getConnectedClientInfo(t *testing.T, conn *sql.DB) string {
	var queryID string
	row := conn.QueryRow("SELECT queryID()")
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&queryID))

	_, err := conn.Exec("SYSTEM FLUSH LOGS")
	require.NoError(t, err)

	var clientName string
	row = conn.QueryRow(fmt.Sprintf("SELECT IF(interface = 2, http_user_agent, client_name) as client_name FROM system.query_log WHERE query_id = '%s'", queryID))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&clientName))

	return clientName
}
