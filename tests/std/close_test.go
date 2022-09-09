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
	"crypto/tls"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdConnClose(t *testing.T) {
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
	conn := GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
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
		TLS: tlsConfig,
	})
	require.NoError(t, conn.Ping())
	var one int
	require.NoError(t, conn.QueryRow("SELECT 1").Scan(&one))
	assert.NoError(t, conn.Close())
}
