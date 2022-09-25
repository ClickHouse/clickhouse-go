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
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
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

// Issue https://github.com/ClickHouse/clickhouse-go/issues/761
func TestQueryCancel(t *testing.T) {
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
		TLS:          tlsConfig,
		MaxOpenConns: 1,
		MaxIdleConns: 1,
		Debug:        true,
	})
	require.NoError(t, err)
	// Issue a query which will take 3 secs, cancel after 1 and reissue a query which take 3 secs - check response is q2, not q1

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond*time.Duration(500)))
	defer cancel()
	var queryId uint16
	// query 1
	err = conn.QueryRow(ctx, "SELECT sleep(3) + 1 as query_id").Scan(&queryId)
	require.Error(t, err)
	// query 2
	err = conn.QueryRow(context.Background(), "SELECT sleep(3) + 2 as query_id").Scan(&queryId)
	require.NoError(t, err)
	require.Equal(t, uint16(2), queryId)

	// what if the cancel timeouts?
}
