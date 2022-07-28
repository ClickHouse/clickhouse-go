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
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdConn(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if assert.NoError(t, err) {
					if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
						if assert.NoError(t, conn.Close()) {
							t.Log(conn.Stats())
						}
					}
				}
			}
		})
	}
}

func TestStdConnFailover(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9000", "Http": "http://127.0.0.1:8124,127.0.0.1:8125,127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {

			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
					t.Log(conn.PingContext(context.Background()))
				}
			}
		})
	}
}

func TestStdConnFailoverConnOpenRoundRobin(t *testing.T) {
	dsns := map[string]string{
		"Native": "clickhouse://127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003,127.0.0.1:9004,127.0.0.1:9005,127.0.0.1:9006,127.0.0.1:9000/?connection_open_strategy=round_robin",
		"Http":   "http://127.0.0.1:8124,127.0.0.1:8125,127.0.0.1:8126,127.0.0.1:8127,127.0.0.1:8128,127.0.0.1:8129,127.0.0.1:8123/?connection_open_strategy=round_robin",
	}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
					t.Log(conn.PingContext(context.Background()))
				}
			}
		})
	}
}

func TestStdPingDeadline(t *testing.T) {
	dsns := map[string]string{
		"Native": "clickhouse://127.0.0.1:9000",
		"Http":   "http://127.0.0.1:8123",
	}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				if err := conn.PingContext(ctx); assert.Error(t, err) {
					assert.Equal(t, err, context.DeadlineExceeded)
				}
			}
		})
	}
}

func TestStdConnAuth(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000?username=default&password=", "Http": "http://127.0.0.1:8123?username=default&password="}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if conn, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) {
				if assert.NoError(t, err) {
					if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
						if assert.NoError(t, conn.Close()) {
							t.Log(conn.Stats())
						}
					}
				}
			}
		})
	}
}
