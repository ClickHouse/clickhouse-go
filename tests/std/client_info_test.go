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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"runtime"
	"strconv"
	"testing"
)

func TestClientInfo(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)

			var queryID string
			row := conn.QueryRow("SELECT queryID()")
			require.NoError(t, row.Err())
			require.NoError(t, row.Scan(&queryID))

			_, err = conn.Exec("SYSTEM FLUSH LOGS")
			require.NoError(t, err)

			var clientName string
			row = conn.QueryRow("SELECT IF(interface = 2, http_user_agent, client_name) as client_name FROM system.query_log WHERE query_id = " + queryID)
			require.NoError(t, row.Err())
			require.NoError(t, row.Scan(&clientName))

			// e.g. tests/dev clickhouse-go/2.5.1 (database/sql; lv:go/1.19.3; os:darwin)
			expectedClientName := fmt.Sprintf(
				"tests/dev %s/%d.%d.%d (database/sql; lv:go/%s; os:%s)",
				clickhouse.ClientName,
				clickhouse.ClientVersionMajor,
				clickhouse.ClientVersionMinor,
				clickhouse.ClientVersionPatch,
				runtime.Version()[2:],
				runtime.GOOS,
			)
			assert.Equal(t, expectedClientName, clientName)
		})
	}
}
