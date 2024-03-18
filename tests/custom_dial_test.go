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
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestCustomDialContext(t *testing.T) {
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	var (
		dialCount int
	)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			if tlsConfig != nil {
				return tls.DialWithDialer(&net.Dialer{Timeout: time.Duration(30) * time.Second}, "tcp", addr, tlsConfig)
			}
			return d.DialContext(ctx, "tcp", addr)
		},
		TLS: tlsConfig,
	})
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, conn.Ping(ctx))
	assert.Equal(t, 1, dialCount)
	ctx1, cancel := context.WithCancel(ctx)

	go func() {
		cancel()
	}()
	start := time.Now()
	// query is cancelled with context
	if err = conn.QueryRow(ctx1, "SELECT sleep(3)").Scan(); assert.Error(t, err, "context cancelled") {
		assert.Equal(t, 1, dialCount)
	}
	assert.True(t, time.Since(start) < time.Second)
}

func TestCustomHTTPDialContext(t *testing.T) {
	SkipOnCloud(t, "Unstable keep-alive on Cloud")

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	var (
		dialCount int
	)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.HttpPort
	var tlsConfig *tls.Config
	if useSSL {
		port = env.HttpsPort
		tlsConfig = &tls.Config{}
	}
	connector := clickhouse.Connector(&clickhouse.Options{
		Addr:     []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Protocol: clickhouse.HTTP,
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		TLS: tlsConfig,
	})
	conn, err := connector.Connect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, conn)
	db := sql.OpenDB(connector)
	require.Equal(t, 1, dialCount)
	err = db.Ping()
	require.NoError(t, err)
}
