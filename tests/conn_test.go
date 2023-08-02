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
	"crypto/tls"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))
	require.NoError(t, conn.Close())
	t.Log(conn.Stats())
	t.Log(conn.ServerVersion())
	t.Log(conn.Ping(context.Background()))
}

func TestBadConn(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9790"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		MaxOpenConns: 2,
	})
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		if err := conn.Ping(context.Background()); assert.Error(t, err) {
			assert.Contains(t, err.Error(), "connect: connection refused")
		}
	}
}

func TestConnFailover(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{
			"127.0.0.1:9001",
			"127.0.0.1:9002",
			fmt.Sprintf("%s:%d", env.Host, port),
		},
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
	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))
	t.Log(conn.ServerVersion())
	t.Log(conn.Ping(context.Background()))
}

func TestConnFailoverConnOpenRoundRobin(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{
			"127.0.0.1:9001",
			"127.0.0.1:9002",
			fmt.Sprintf("%s:%d", env.Host, port),
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		TLS:              tlsConfig,
	})
	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))
	t.Log(conn.ServerVersion())
	t.Log(conn.Ping(context.Background()))
}

func TestPingDeadline(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err = conn.Ping(ctx)
	require.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestReadDeadline(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ReadTimeout: time.Duration(-1) * time.Second,
		TLS:         tlsConfig,
	})
	require.NoError(t, err)
	err = conn.Ping(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
	// check we can override with context
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*time.Duration(10)))
	defer cancel()
	require.NoError(t, conn.Ping(ctx))
}

func TestQueryDeadline(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ReadTimeout: time.Duration(-1) * time.Second,
		TLS:         tlsConfig,
	})
	require.NoError(t, err)
	var count uint64
	err = conn.QueryRow(context.Background(), "SELECT count() FROM numbers(10000000)").Scan(&count)
	require.Error(t, err)
	assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestBlockBufferSize(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		TLS:             tlsConfig,
		BlockBufferSize: 100,
	})
	require.NoError(t, err)
	var count uint64
	rows, err := conn.Query(clickhouse.Context(context.Background(), clickhouse.WithBlockBufferSize(50)), "SELECT number FROM numbers(10000000)")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		require.NoError(t, rows.Scan(&count))
		i++
	}
	require.Equal(t, 10000000, i)
}

func TestConnCustomDialStrategy(t *testing.T) {
	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	opts := ClientOptionsFromEnv(env, clickhouse.Settings{})
	validAddr := opts.Addr[0]
	opts.Addr = []string{"invalid.host:9001"}

	opts.DialStrategy = func(ctx context.Context, connID int, opts *clickhouse.Options, dial clickhouse.Dial) (clickhouse.DialResult, error) {
		return dial(ctx, validAddr, opts)
	}

	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)

	require.NoError(t, err)
	require.NoError(t, conn.Ping(context.Background()))
	require.NoError(t, conn.Close())
}

func TestEmptyDatabaseConfig(t *testing.T) {
	runInDocker, _ := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if !runInDocker {
		t.Skip("Skip test in cloud environment.")
	}

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Username: env.Username,
			Password: env.Password,
		},
		TLS: tlsConfig,
	}
	conn, err := GetConnectionWithOptions(options)
	require.NoError(t, err)

	// Setup
	err = conn.Exec(context.Background(), `DROP DATABASE IF EXISTS "default"`)
	require.NoError(t, err)

	defer func() {
		// Tear down
		err = conn.Exec(context.Background(), `CREATE DATABASE "default"`)
		require.NoError(t, err)
	}()

	anotherConn, err := GetConnectionWithOptions(options)
	require.NoError(t, err)
	err = anotherConn.Ping(context.Background())
	require.NoError(t, err)
}

func TestCustomSettings(t *testing.T) {
	runInDocker, _ := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if !runInDocker {
		t.Skip("Skip test in cloud environment.") // todo configure cloud instance with custom settings
	}

	conn, err := GetNativeConnection(clickhouse.Settings{
		"custom_setting": clickhouse.CustomSetting{"custom_value"},
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	t.Run("get existing custom setting value", func(t *testing.T) {
		row := conn.QueryRow(context.Background(), "SELECT getSetting('custom_setting')")
		require.NoError(t, row.Err())

		var setting string
		require.NoError(t, row.Scan(&setting))
		require.Equal(t, "custom_value", setting)
	})

	t.Run("get non-existing custom setting value", func(t *testing.T) {
		row := conn.QueryRow(context.Background(), "SELECT getSetting('custom_non_existing_setting')")
		require.ErrorContains(t, row.Err(), "Unknown setting custom_non_existing_setting")
	})

	t.Run("get custom setting value from query context", func(t *testing.T) {
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"custom_query_setting": clickhouse.CustomSetting{"custom_query_value"},
		}))

		row := conn.QueryRow(ctx, "SELECT getSetting('custom_query_setting')")
		require.NoError(t, row.Err())

		var setting string
		require.NoError(t, row.Scan(&setting))
		require.Equal(t, "custom_query_value", setting)
	})
}

func TestConnectionExpiresIdleConnection(t *testing.T) {
	runInDocker, _ := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if !runInDocker {
		t.Skip("Skip test in cloud environment. This test is not stable in cloud environment, due to race conditions.")
	}

	// given
	ctx := context.Background()
	testEnv, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	baseConn, err := TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	expectedConnections := getActiveConnections(t, baseConn)

	// when the client is configured to expire idle connections after 1/10 of a second
	opts := ClientOptionsFromEnv(testEnv, clickhouse.Settings{})
	opts.MaxIdleConns = 20
	opts.MaxOpenConns = 20
	opts.ConnMaxLifetime = time.Second / 10
	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)

	// run 1000 queries in parallel
	var wg sync.WaitGroup
	const selectToRunAtOnce = 1000
	for i := 0; i < selectToRunAtOnce; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := conn.Query(ctx, "SELECT 1")
			require.NoError(t, err)

			r.Close()
		}()
	}
	wg.Wait()

	// then we expect that all connections will be closed when they are idle
	// retrying for 10 seconds to make sure that the connections are closed
	assert.Eventuallyf(t, func() bool {
		return getActiveConnections(t, baseConn) == expectedConnections
	}, time.Second*10, opts.ConnMaxLifetime, "expected connections to be reset back to %d", expectedConnections)
}

func getActiveConnections(t *testing.T, client clickhouse.Conn) (conns int64) {
	ctx := context.Background()
	r := client.QueryRow(ctx, "SELECT sum(value) as conns FROM system.metrics WHERE metric LIKE '%Connection'")
	require.NoError(t, r.Err())
	require.NoError(t, r.Scan(&conns))
	return conns
}

func TestConnectionCloseIdle(t *testing.T) {
	runInDocker, _ := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_DOCKER", "true"))
	if !runInDocker {
		t.Skip("Skip test in cloud environment. This test is not stable in cloud environment, due to race conditions.")
	}
	testEnv, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	baseGoroutine := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		ctx := context.Background()
		conn, err := TestClientWithDefaultSettings(testEnv)
		require.NoError(t, err)
		err = conn.Ping(ctx)
		conn.Close()
		require.NoError(t, err)
	}
	time.Sleep(100 * time.Millisecond) // wait for all connections closed
	finalGoroutine := runtime.NumGoroutine()

	// it can be equal to baseGoroutine, but usually it's not
	// it's around baseGoroutine + 1 or 2 due to other features spawning goroutines
	// + 4 is a value from the observation of the test failure in CI
	assert.LessOrEqual(t, finalGoroutine, baseGoroutine+4)
}
