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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdConnCheck(t *testing.T) {
	const (
		ddl = `
		CREATE TABLE clickhouse_test_conn_check (
			Value String
		) Engine MergeTree() ORDER BY tuple()
		`
		dml = "INSERT INTO `clickhouse_test_conn_check` VALUES "
	)

	env, err := GetStdTestEnvironment()
	require.NoError(t, err)

	dsns := map[clickhouse.Protocol]string{clickhouse.Native: fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		clickhouse.HTTP: fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[clickhouse.Protocol]string{clickhouse.Native: fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			clickhouse.HTTP: fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		if name == clickhouse.Native && useSSL {
			//TODO: test fails over native and SSL - requires investigation
			continue
		}
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			connect, err := GetConnectionFromDSNWithSessionID(dsn, "conn_test_session")
			require.NoError(t, err)
			// We can only change the settings at the connection level.
			// If we have only one connection, we change the settings specifically for that connection.
			connect.SetMaxOpenConns(1)
			_, err = connect.Exec("DROP TABLE IF EXISTS clickhouse_test_conn_check")
			require.NoError(t, err)
			_, err = connect.Exec(ddl)
			require.NoError(t, err)
			_, err = connect.Exec("set idle_connection_timeout=1")
			assert.NoError(t, err)
			//The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes
			_, err = connect.Exec("set tcp_keep_alive_timeout=0")
			assert.NoError(t, err)

			time.Sleep(1100 * time.Millisecond)
			ctx := context.Background()
			tx, err := connect.BeginTx(ctx, nil)
			assert.NoError(t, err)

			_, err = tx.PrepareContext(ctx, dml)
			assert.NoError(t, err)
			assert.NoError(t, tx.Commit())

			_, err = connect.Exec("DROP TABLE IF EXISTS clickhouse_test_conn_check")
			require.NoError(t, err)
			require.NoError(t, connect.Close())
		})
	}
}
