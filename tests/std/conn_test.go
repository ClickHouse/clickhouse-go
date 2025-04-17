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
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	testStdConnFailover(t, "")
}

func TestStdConnFailoverRoundRobin(t *testing.T) {
	testStdConnFailover(t, "round_robin")
}

func TestStdConnFailoverRandom(t *testing.T) {
	rand.Seed(85206178671753423)
	defer clickhouse_tests.ResetRandSeed()
	testStdConnFailover(t, "random")
}

func testStdConnFailover(t *testing.T, openStrategy string) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	nativePort := env.Port
	httpPort := env.HttpPort
	argsList := []string{}
	scheme := "http"
	if useSSL {
		nativePort = env.SslPort
		httpPort = env.HttpsPort
		argsList = append(argsList, "secure=true")
		scheme = "https"
	}
	if len(openStrategy) > 0 {
		argsList = append(argsList, fmt.Sprintf("connection_open_strategy=%s", openStrategy))
	}
	args := ""
	if len(argsList) > 0 {
		args = "?" + strings.Join(argsList, "&")
	}
	dsns := map[string]string{
		"Native": fmt.Sprintf("clickhouse://%s:%s@127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006,%s:%d/%s", env.Username, env.Password, env.Host, nativePort, args),
		"Http":   fmt.Sprintf("%s://%s:%s@127.0.0.1:8124,127.0.0.1:8125,127.0.0.1:8126,127.0.0.1:8127,127.0.0.1:8128,127.0.0.1:8129,%s:%d/%s", scheme, env.Username, env.Password, env.Host, httpPort, args),
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

func TestHttpConnWithOptions(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t)

	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	nginxEnv, err := clickhouse_tests.CreateNginxReverseProxyTestEnvironment(env)
	defer func() {
		if nginxEnv.NginxContainer != nil {
			nginxEnv.NginxContainer.Terminate(context.Background())
		}
	}()
	require.NoError(t, err)
	conn := GetConnectionWithOptions(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, nginxEnv.HttpPort)},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 5 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		HttpUrlPath: "clickhouse",
	})
	require.NoError(t, conn.Ping())
	var one int
	require.NoError(t, conn.QueryRow("SELECT 1").Scan(&one))
	assert.NoError(t, conn.Close())
}

func TestEmptyDatabaseConfig(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t)

	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	dsns := map[string]string{
		"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s", env.Host, env.Port, env.Username, env.Password),
		"Http":   fmt.Sprintf("http://%s:%d?username=%s&password=%s", env.Host, env.HttpPort, env.Username, env.Password),
	}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if useSSL {
		dsns = map[string]string{
			"Native": fmt.Sprintf("clickhouse://%s:%d?username=%s&password=%s&secure=true", env.Host, env.Port, env.Username, env.Password),
			"Http":   fmt.Sprintf("https://%s:%d?username=%s&password=%s&secure=true", env.Host, env.HttpPort, env.Username, env.Password),
		}
	}

	setupConn, err := sql.Open("clickhouse", dsns["Native"])
	require.NoError(t, err)

	// Setup
	_, err = setupConn.ExecContext(context.Background(), `DROP DATABASE IF EXISTS "default"`)
	require.NoError(t, err)

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := sql.Open("clickhouse", dsn)
			require.NoError(t, err)
			err = conn.Ping()
			require.NoError(t, err)
		})
	}

	// Tear down
	_, err = setupConn.ExecContext(context.Background(), `CREATE DATABASE "default"`)
	require.NoError(t, err)
}

func TestHTTPProxy(t *testing.T) {
	t.Skip("test is flaky, tinyproxy container can't be started in CI")

	clickhouse_tests.SkipOnCloud(t)

	proxyEnv, err := clickhouse_tests.CreateTinyProxyTestEnvironment(t)
	defer func() {
		if proxyEnv.Container != nil {
			proxyEnv.Container.Terminate(context.Background())
		}
	}()
	require.NoError(t, err)

	proxyURL := proxyEnv.ProxyUrl(t)

	os.Setenv("HTTP_PROXY", proxyURL)
	os.Setenv("HTTPS_PROXY", proxyURL)
	defer func() {
		os.Unsetenv("HTTP_PROXY")
		os.Unsetenv("HTTPS_PROXY")
	}()

	logs, err := proxyEnv.Container.Logs(context.Background())
	require.NoError(t, err)
	defer logs.Close()
	scanner := bufio.NewScanner(logs)

	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := GetStdDSNConnection(clickhouse.HTTP, useSSL, nil)

	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Ping())

	assert.Eventually(t, func() bool {
		if !scanner.Scan() {
			return false
		}

		text := scanner.Text()
		t.Log(text)
		return strings.Contains(text, fmt.Sprintf("Established connection to host \"%s\"", ""))
	}, 60*time.Second, time.Millisecond, "proxy logs should contain clickhouse.cloud instance host")
}

func TestCustomSettings(t *testing.T) {
	clickhouse_tests.SkipOnCloud(t)

	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdOpenDBConnection(
				protocol,
				clickhouse.Settings{
					"custom_setting": clickhouse.CustomSetting{"custom_value"},
				},
				nil,
				nil,
			)
			require.NoError(t, err)

			t.Run("get existing custom setting value", func(t *testing.T) {
				row := conn.QueryRowContext(context.Background(), "SELECT getSetting('custom_setting')")
				require.NoError(t, row.Err())

				var setting string
				assert.NoError(t, row.Scan(&setting))
				assert.Equal(t, "custom_value", setting)
			})

			t.Run("get non-existing custom setting value", func(t *testing.T) {
				row := conn.QueryRowContext(context.Background(), "SELECT getSetting('custom_non_existing_setting')")
				assert.Contains(t, strings.ReplaceAll(row.Err().Error(), "'", ""), "Unknown setting custom_non_existing_setting")
			})

			t.Run("get custom setting value from query context", func(t *testing.T) {
				ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
					"custom_query_setting": clickhouse.CustomSetting{"custom_query_value"},
				}))

				row := conn.QueryRowContext(ctx, "SELECT getSetting('custom_query_setting')")
				assert.NoError(t, row.Err())

				var setting string
				assert.NoError(t, row.Scan(&setting))
				assert.Equal(t, "custom_query_value", setting)
			})
		})
	}
}

func TestStdJWTAuth(t *testing.T) {
	clickhouse_tests.SkipNotCloud(t)

	protocols := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	for name, protocol := range protocols {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			jwt := clickhouse_tests.GetEnv("CLICKHOUSE_JWT", "")
			getJWT := func(ctx context.Context) (string, error) {
				return jwt, nil
			}

			conn, err := GetOpenDBConnectionJWT(testSet, protocol, nil, &tls.Config{}, getJWT)
			require.NoError(t, err)
			conn.SetMaxOpenConns(1)
			conn.SetConnMaxLifetime(1000 * time.Millisecond)
			conn.SetConnMaxIdleTime(1000 * time.Millisecond)
			conn.SetMaxIdleConns(1)

			// Token works
			require.NoError(t, conn.PingContext(context.Background()))

			// Wait for connection to timeout
			time.Sleep(1500 * time.Millisecond)

			// Break the token
			jwt = "broken_jwt"

			// Next ping should fail
			require.Error(t, conn.PingContext(context.Background()))

			require.NoError(t, conn.Close())
		})
	}
}

func TestJWTAuthHTTPOverride(t *testing.T) {
	clickhouse_tests.SkipNotCloud(t)

	getJWT := func(ctx context.Context) (string, error) {
		return clickhouse_tests.GetEnv("CLICKHOUSE_JWT", ""), nil
	}

	conn, err := GetOpenDBConnectionJWT(testSet, clickhouse.HTTP, nil, &tls.Config{}, getJWT)
	require.NoError(t, err)
	conn.SetMaxOpenConns(1)
	conn.SetConnMaxLifetime(1000 * time.Millisecond)
	conn.SetConnMaxIdleTime(1000 * time.Millisecond)
	conn.SetMaxIdleConns(1)

	// Token works
	require.NoError(t, conn.PingContext(context.Background()))

	// Break the token via temporary override
	ctx := clickhouse.Context(context.Background(), clickhouse.WithJWT("broken_jwt"))
	// Next ping with context should fail
	require.Error(t, conn.PingContext(ctx))

	// Next ping with client-level JWT should succeed
	require.NoError(t, conn.PingContext(context.Background()))

	require.NoError(t, conn.Close())
}
