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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func TestQuotedDDL(t *testing.T) {
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
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout: time.Second * 120,
		TLS:         tlsConfig,
	})
	ctx := context.Background()
	require.NoError(t, err)
	require.NoError(t, conn.Ping(ctx))
	if err := CheckMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = "CREATE TABLE `test_string` (`1` String) Engine Memory"

	defer func() {
		conn.Exec(ctx, "DROP TABLE `test_string`")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO `test_string`")
	require.NoError(t, err)
	require.NoError(t, batch.Append("A"))
	require.NoError(t, batch.Send())
}
