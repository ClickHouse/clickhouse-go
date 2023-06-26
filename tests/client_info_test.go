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

package tests

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"testing"
)

func TestClientInfo(t *testing.T) {
	expectedClientProduct := fmt.Sprintf(
		"%s/%d.%d.%d (lv:go/%s; os:%s)",
		clickhouse.ClientName,
		clickhouse.ClientVersionMajor,
		clickhouse.ClientVersionMinor,
		clickhouse.ClientVersionPatch,
		runtime.Version()[2:],
		runtime.GOOS,
	)

	testCases := map[string]struct {
		expectedClientInfo string
		clientInfo         clickhouse.ClientInfo
	}{
		"no additional products": {
			// e.g. clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			expectedClientProduct,
			clickhouse.ClientInfo{},
		},
		"one additional product": {
			// e.g. tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("tests/dev %s", expectedClientProduct),
			clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "tests",
						Version: "dev",
					},
				},
			},
		},
		"two additional products": {
			// e.g. product/version tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			fmt.Sprintf("product/version tests/dev %s", expectedClientProduct),
			clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{
						Name:    "product",
						Version: "version",
					},
					{
						Name:    "tests",
						Version: "dev",
					},
				},
			},
		},
	}

	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := ClientOptionsFromEnv(env, clickhouse.Settings{})
			opts.ClientInfo = testCase.clientInfo

			conn, err := clickhouse.Open(&opts)
			require.NoError(t, err)

			actualClientInfo := getConnectedClientInfo(t, conn)
			assert.Equal(t, testCase.expectedClientInfo, actualClientInfo)
		})
	}
}

func getConnectedClientInfo(t *testing.T, conn driver.Conn) string {
	var queryID string
	row := conn.QueryRow(context.TODO(), "SELECT queryID()")
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&queryID))

	err := conn.Exec(context.TODO(), "SYSTEM FLUSH LOGS")
	require.NoError(t, err)

	var clientName string
	row = conn.QueryRow(context.TODO(), fmt.Sprintf("SELECT IF(interface = 2, http_user_agent, client_name) as client_name FROM system.query_log WHERE query_id = '%s'", queryID))
	require.NoError(t, row.Err())
	require.NoError(t, row.Scan(&clientName))

	return clientName
}
