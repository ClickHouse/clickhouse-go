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
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConn(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		require.NoError(t, conn.Ping(context.Background()))
		t.Log(conn.Stats())
		t.Log(conn.ServerVersion())
		require.NoError(t, conn.Close())
	})
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
	testConnFailover(t, clickhouse.ConnOpenInOrder)
}

func TestConnFailoverRoundRobin(t *testing.T) {
	testConnFailover(t, clickhouse.ConnOpenRoundRobin)
}

func TestConnFailoverRandom(t *testing.T) {
	rand.Seed(85206178671753423)
	defer ResetRandSeed()
	testConnFailover(t, clickhouse.ConnOpenRandom)
}

func testConnFailover(t *testing.T, connOpenStrategy clickhouse.ConnOpenStrategy) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		if connOpenStrategy == clickhouse.ConnOpenRandom {
			SkipOnHTTP(t, protocol, "random seed")
		}

		env, err := GetNativeTestEnvironment()
		require.NoError(t, err)
		useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)
		port := env.Port
		if protocol == clickhouse.HTTP {
			port = env.HttpPort
		}
		var tlsConfig *tls.Config
		if useSSL {
			tlsConfig = &tls.Config{}
			port = env.SslPort
			if protocol == clickhouse.HTTP {
				port = env.HttpsPort
			}
		}
		options := clickhouse.Options{
			Protocol:         protocol,
			ConnOpenStrategy: connOpenStrategy,
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
		}

		conn, err := GetConnectionWithOptions(&options)
		require.NoError(t, err)
		require.NoError(t, conn.Ping(context.Background()))
		t.Log(conn.ServerVersion())
		t.Log(conn.Ping(context.Background()))
	})
}

func TestPingDeadline(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		err = conn.Ping(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
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
	require.NoError(t, conn.Close())
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
	require.NoError(t, conn.Close())
}

func TestBlockBufferSize(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		env, err := GetNativeTestEnvironment()
		require.NoError(t, err)
		useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)
		port := env.Port
		if protocol == clickhouse.HTTP {
			port = env.HttpPort
		}
		var tlsConfig *tls.Config
		if useSSL {
			tlsConfig = &tls.Config{}
			port = env.SslPort
			if protocol == clickhouse.HTTP {
				port = env.HttpsPort
			}
		}
		conn, err := GetConnectionWithOptions(&clickhouse.Options{
			Protocol: protocol,
			Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
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
		rows, err := conn.Query(clickhouse.Context(context.Background(), clickhouse.WithBlockBufferSize(50)), "SELECT number FROM numbers(1000000)")
		require.NoError(t, err)
		i := 0
		for rows.Next() {
			require.NoError(t, rows.Scan(&count))
			i++
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())
		require.Equal(t, 1000000, i)
		require.NoError(t, conn.Close())
	})
}

func TestConnAcquireRelease(t *testing.T) {
	SkipOnCloud(t, "requires low latency")

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		env, err := GetNativeTestEnvironment()
		require.NoError(t, err)
		useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
		require.NoError(t, err)
		port := env.Port
		if protocol == clickhouse.HTTP {
			port = env.HttpPort
		}
		var tlsConfig *tls.Config
		if useSSL {
			tlsConfig = &tls.Config{}
			port = env.SslPort
			if protocol == clickhouse.HTTP {
				port = env.HttpsPort
			}
		}
		conn, err := GetConnectionWithOptions(&clickhouse.Options{
			Protocol: protocol,
			Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: env.Username,
				Password: env.Password,
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			TLS:          tlsConfig,
			DialTimeout:  100 * time.Millisecond, // Fast acquire failure
			MaxIdleConns: 1,
			MaxOpenConns: 1, // Force one connection
		})
		require.NoError(t, err)

		// 2 sequential calls
		require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
		require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))

		// 2 concurrent calls
		var startedWg sync.WaitGroup
		var finishedWg sync.WaitGroup
		startedWg.Add(1)
		finishedWg.Add(1)
		go func() {
			startedWg.Done()
			require.NoError(t, conn.Exec(context.Background(), "SELECT sleep(1)"))
			finishedWg.Done()
		}()
		// Wait for goroutine to start
		startedWg.Wait()
		// Wait for query to start
		time.Sleep(100 * time.Millisecond)
		// Try to acquire another connection, expecting an error
		require.Error(t, conn.Exec(context.Background(), "SELECT 1"))
		// Wait for goroutine to exit
		finishedWg.Wait()

		require.NoError(t, conn.Close())
	})
}

func TestConnCustomDialStrategy(t *testing.T) {
	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	opts := ClientOptionsFromEnv(env, clickhouse.Settings{}, false)
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
	SkipOnCloud(t)

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
	defer conn.Close()
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
	defer anotherConn.Close()
	require.NoError(t, err)
	err = anotherConn.Ping(context.Background())
	require.NoError(t, err)
}

func TestCustomSettings(t *testing.T) {
	SkipOnCloud(t, "Custom settings are not supported on ClickHouse Cloud")

	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, clickhouse.Settings{
			"custom_setting": clickhouse.CustomSetting{"custom_value"},
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)

		t.Run("get existing custom setting value", func(t *testing.T) {
			row := conn.QueryRow(context.Background(), "SELECT getSetting('custom_setting')")
			require.NoError(t, row.Err())

			var setting string
			assert.NoError(t, row.Scan(&setting))
			assert.Equal(t, "custom_value", setting)
		})

		t.Run("get non-existing custom setting value", func(t *testing.T) {
			row := conn.QueryRow(context.Background(), "SELECT getSetting('custom_non_existing_setting')")
			assert.Contains(t, strings.ReplaceAll(row.Err().Error(), "'", ""), "Unknown setting custom_non_existing_setting")
		})

		t.Run("get custom setting value from query context", func(t *testing.T) {
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				"custom_query_setting": clickhouse.CustomSetting{"custom_query_value"},
			}))

			row := conn.QueryRow(ctx, "SELECT getSetting('custom_query_setting')")
			assert.NoError(t, row.Err())

			var setting string
			assert.NoError(t, row.Scan(&setting))
			assert.Equal(t, "custom_query_value", setting)
		})
	})
}

func TestConnectionExpiresIdleConnection(t *testing.T) {
	SkipOnCloud(t)

	// given
	ctx := context.Background()
	testEnv, err := GetTestEnvironment(testSet)
	require.NoError(t, err)

	baseConn, err := TestClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	expectedConnections := getActiveConnections(t, baseConn)

	// when the client is configured to expire idle connections after 1/10 of a second
	opts := ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, false)
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
			require.NoError(t, r.Close())
			require.NoError(t, r.Err())
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
	SkipOnCloud(t)

	testEnv, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	baseGoroutine := runtime.NumGoroutine()
	for i := 0; i < 100; i++ {
		ctx := context.Background()
		conn, err := TestClientWithDefaultSettings(testEnv)
		require.NoError(t, err)
		err = conn.Ping(ctx)
		require.NoError(t, err)
		require.NoError(t, conn.Close())
	}
	time.Sleep(100 * time.Millisecond) // wait for all connections closed
	finalGoroutine := runtime.NumGoroutine()

	// it can be equal to baseGoroutine, but usually it's not
	// it's around baseGoroutine + 1 or 2 due to other features spawning goroutines
	// + 4 is a value from the observation of the test failure in CI
	assert.LessOrEqual(t, finalGoroutine, baseGoroutine+4)
}

func TestFreeBufOnConnRelease(t *testing.T) {
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
		TLS:                  tlsConfig,
		FreeBufOnConnRelease: true,
		// ensure we'll reuse the underlying connection:
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	require.NoError(t, err)

	err = conn.Exec(context.Background(), "CREATE TABLE TestFreeBufOnConnRelease (Col1 String) Engine MergeTree() ORDER BY tuple()")
	require.NoError(t, err)

	t.Run("InsertBatch", func(t *testing.T) {
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO TestFreeBufOnConnRelease (Col1) VALUES")
		require.NoError(t, err)
		err = batch.Append("abc")
		require.NoError(t, err)
		err = batch.Send()
		require.NoError(t, err)
	})

	t.Run("ReuseConnection", func(t *testing.T) {
		var result []struct {
			Col1 string
		}
		err = conn.Select(context.Background(), &result, "SELECT Col1 FROM TestFreeBufOnConnRelease")
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.Equal(t, "abc", result[0].Col1)
	})

	err = conn.Exec(context.Background(), "DROP TABLE TestFreeBufOnConnRelease")
	require.NoError(t, err)
}

func TestJWTError(t *testing.T) {
	getJWT := func(ctx context.Context) (string, error) {
		return "", fmt.Errorf("test error")
	}

	conn, err := GetJWTConnection(testSet, nil, nil, 1000*time.Millisecond, getJWT)
	require.NoError(t, err)
	require.ErrorContains(t, conn.Ping(context.Background()), "test error")
}

func TestNativeJWTAuth(t *testing.T) {
	SkipNotCloud(t)

	jwt := GetEnv("CLICKHOUSE_JWT", "")
	getJWT := func(ctx context.Context) (string, error) {
		return jwt, nil
	}

	conn, err := GetJWTConnection(testSet, nil, nil, 1000*time.Millisecond, getJWT)
	require.NoError(t, err)

	// Token works
	require.NoError(t, conn.Ping(context.Background()))

	// Wait for connection to timeout
	time.Sleep(1500 * time.Millisecond)

	// Break the token
	jwt = "broken_jwt"

	// Next ping should fail
	require.Error(t, conn.Ping(context.Background()))

	require.NoError(t, conn.Close())
}
