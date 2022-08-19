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
	"fmt"
	"github.com/stretchr/testify/require"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestCustomDialContext(t *testing.T) {
	port := GetEnv("CLICKHOUSE_PORT", "9000")
	host := GetEnv("CLICKHOUSE_HOST", "localhost")
	username := GetEnv("CLICKHOUSE_USERNAME", "default")
	password := GetEnv("CLICKHOUSE_PASSWORD", "")

	var (
		dialCount int
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%s", host, port)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: username,
				Password: password,
			},
			DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
				dialCount++
				var d net.Dialer
				return d.DialContext(ctx, "tcp", addr)
			},
		})
	)
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
	err = conn.QueryRow(ctx1, "SELECT sleep(10)").Scan()
	require.Error(t, err, "context cancelled")
	assert.Equal(t, 1, dialCount)
	assert.True(t, time.Since(start) < time.Second)
}
