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
	"crypto/tls"
	"database/sql"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdConn(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			require.NoError(t, conn.PingContext(context.Background()))
			require.NoError(t, conn.Close())
			t.Log(conn.Stats())
		})
	}
}

func TestStdConnFailover(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%s@127.0.0.1:9001,127.0.0.1:9002,%s:%d", env.Username, env.Password, env.Host, env.Port),
		"Http": fmt.Sprintf("http://%s:%s@127.0.0.1:8124,127.0.0.1:8125,%s:%d", env.Username, env.Password, env.Host, env.HttpPort)}
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%s@127.0.0.1:9001,127.0.0.1:9002,%s:%d?secure=true", env.Username, env.Password, env.Host, env.SslPort),
			"Http": fmt.Sprintf("https://%s:%s@127.0.0.1:8124,127.0.0.1:8125,%s:%d?secure=true", env.Username, env.Password, env.Host, env.HttpsPort)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {

			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
					t.Log(conn.PingContext(context.Background()))
				}
			}
		})
	}
}

func TestStdConnFailoverConnOpenRoundRobin(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{
		"Native": fmt.Sprintf("clickhouse://%s:%s@127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006,%s:%d/?connection_open_strategy=round_robin", env.Username, env.Password, env.Host, env.Port),
		"Http":   fmt.Sprintf("http://%s:%s@127.0.0.1:8124,127.0.0.1:8125,127.0.0.1:8126,127.0.0.1:8127,127.0.0.1:8128,127.0.0.1:8129,%s:%d/?connection_open_strategy=round_robin", env.Username, env.Password, env.Host, env.HttpPort),
	}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{
			"Native": fmt.Sprintf("clickhouse://%s:%s@127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006,%s:%d/?connection_open_strategy=round_robin&secure=true", env.Username, env.Password, env.Host, env.SslPort),
			"Http":   fmt.Sprintf("https://%s:%s@127.0.0.1:8124,127.0.0.1:8125,127.0.0.1:8126,127.0.0.1:8127,127.0.0.1:8128,127.0.0.1:8129,%s:%d/?connection_open_strategy=round_robin&secure=true", env.Username, env.Password, env.Host, env.HttpsPort),
		}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
					t.Log(conn.PingContext(context.Background()))
				}
			}
		})
	}
}

func TestStdPingDeadline(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{
		"Native": fmt.Sprintf("clickhouse://%s:%s:%s:%d", env.Username, env.Password, env.Host, env.Port),
		"Http":   fmt.Sprintf("http://%s:%s:%s:%d", env.Username, env.Password, env.Host, env.HttpPort),
	}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{
			"Native": fmt.Sprintf("clickhouse://%s:%s:%s:%d?secure=true", env.Username, env.Password, env.Host, env.SslPort),
			"Http":   fmt.Sprintf("http://%s:%s:%s:%d?secure=true", env.Username, env.Password, env.Host, env.HttpsPort),
		}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				if err := conn.PingContext(ctx); assert.Error(t, err) {
					assert.Equal(t, err, context.DeadlineExceeded)
				}
			}
		})
	}
}

func TestStdConnAuth(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := sql.Open("clickhouse", dsn)
			require.NoError(t, err)
			require.NoError(t, conn.PingContext(context.Background()))
			require.NoError(t, conn.Close())
			t.Log(conn.Stats())
		})
	}
}

func TestStdHTTPEmptyResponse(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSN(dsn)
			defer func() {
				conn.Exec("DROP TABLE empty_example")
			}()
			conn.Exec("DROP TABLE IF EXISTS empty_example")
			_, err = conn.Exec(`
				CREATE TABLE empty_example (
					  Col1 UInt64
					, Col2 String
					, Col3 FixedString(3)
					, Col4 UUID
					, Col5 Map(String, UInt64)
					, Col6 Array(String)
					, Col7 Tuple(String, UInt64, Array(Map(String, UInt64)))
					, Col8 DateTime
				) Engine = MergeTree() ORDER BY tuple()
			`)
			require.NoError(t, err)
			rows, err := conn.Query("SELECT Col1 FROM empty_example")
			require.NoError(t, err)
			count := 0
			for rows.Next() {
				count++
			}
			assert.Equal(t, 0, count)
			var col1 uint64
			// will return with no rows in result set
			err = conn.QueryRow("SELECT * FROM empty_example").Scan(&col1)
			require.Error(t, err)
			assert.Equal(t, "sql: no rows in result set", err.Error())
		})
	}
}

func TestStdConnector(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	connector := clickhouse.Connector(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		TLS: tlsConfig,
	})
	require.NotNil(t, connector)
	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, conn)
	db := sql.OpenDB(connector)
	err = db.Ping()
	require.NoError(t, err)
}

func TestBlockBufferSize(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			dsn := fmt.Sprintf("%s&block_buffer_size=100", dsn)
			conn, err := GetConnectionFromDSN(dsn)
			require.NoError(t, err)
			var count uint64
			rows, err := conn.Query("SELECT number FROM numbers(1000000)")
			require.NoError(t, err)
			i := 0
			for rows.Next() {
				require.NoError(t, rows.Scan(&count))
				i++
			}
			require.Equal(t, 1000000, i)
		})
	}
}

func TestMaxExecutionTime(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http": fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password)}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.SslPort, env.Username, env.Password),
			"Http": fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)}
	}
	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			dsn := fmt.Sprintf("%s&max_execution_time=2", dsn)
			conn, err := GetConnectionFromDSN(dsn)
			require.NoError(t, err)
			rows, err := conn.Query("SELECT sleep(3), number FROM numbers(10)")
			switch name {
			case "Http":
				assert.Error(t, err)
			case "Native":
				assert.NoError(t, err)
				for rows.Next() {

				}
				assert.Error(t, rows.Err())
			}
		})
	}
}
