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
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestCustomDialContext(t *testing.T) {
	var (
		dialCount int
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
				dialCount++
				var d net.Dialer
				return d.DialContext(ctx, "tcp", addr)
			},
		})
	)
	if !assert.NoError(t, err) {
		return
	}
	ctx := context.Background()
	if err := conn.Ping(ctx); assert.NoError(t, err) {
		assert.Equal(t, 1, dialCount)
	}

	ctx1, cancel := context.WithCancel(ctx)

	go func() {
		cancel()
	}()
	start := time.Now()
	// query is cancelled with context
	if err = conn.QueryRow(ctx1, "SELECT sleep(10)").Scan(); assert.Error(t, err, "context cancelled") {
		assert.Equal(t, 1, dialCount)
	}
	assert.True(t, time.Since(start) < time.Second)
}
