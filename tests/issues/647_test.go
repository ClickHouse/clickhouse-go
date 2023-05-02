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

package issues

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestIssue647(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	port := env.Port
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		TLS: tlsConfig,
	}
	conn, err := clickhouse_tests.GetConnectionWithOptions(options)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, conn.Ping(ctx))
	//reuse options
	conn2, err := clickhouse_tests.GetConnectionWithOptions(options)
	require.NoError(t, err)
	require.NoError(t, conn2.Ping(ctx))
}

func TestIssue647_OpenDB(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	port := env.Port
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		TLS: tlsConfig,
	}
	conn := clickhouse.OpenDB(options)
	require.NoError(t, conn.Ping())
	//reuse options
	conn2 := clickhouse.OpenDB(options)
	require.NoError(t, conn2.Ping())
	// allow nil to be parsed - should work if ClickHouse was available on 9000
	//conn3 := clickhouse.OpenDB(nil)
	//require.NoError(t, conn3.Ping())
}

func Test647_Connector(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	port := env.Port
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		TLS: tlsConfig,
	}
	conn := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn).Ping())
	// reuse options
	conn2 := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn2).Ping())
	// allow nil to be parsed - should work if ClickHouse was available on 9000
	//conn3 := clickhouse.Connector(nil)
	//require.NoError(t, sql.OpenDB(conn3).Ping())
}
