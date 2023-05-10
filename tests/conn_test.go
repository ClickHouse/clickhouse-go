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
	"strconv"
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

	opts := clientOptionsFromEnv(env, clickhouse.Settings{})
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
