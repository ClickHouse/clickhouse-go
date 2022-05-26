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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			if assert.NoError(t, conn.Close()) {
				t.Log(conn.Stats())
				t.Log(conn.ServerVersion())
				t.Log(conn.Ping(context.Background()))
			}
		}
	}
}

func TestBadConn(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9790"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		MaxOpenConns: 2,
		//Debug: true,
	})
	if assert.NoError(t, err) {
		for i := 0; i < 20; i++ {
			if err := conn.Ping(context.Background()); assert.Error(t, err) {
				assert.Contains(t, err.Error(), "connect: connection refused")
			}
		}
	}
}
func TestConnFailover(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			"127.0.0.1:9001",
			"127.0.0.1:9002",
			"127.0.0.1:9000",
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//	Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			t.Log(conn.ServerVersion())
			t.Log(conn.Ping(context.Background()))
		}
	}
}
func TestConnFailoverConnOpenRoundRobin(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			"127.0.0.1:9001",
			"127.0.0.1:9002",
			"127.0.0.1:9000",
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		ConnOpenStrategy: clickhouse.ConnOpenRoundRobin,
		//	Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			t.Log(conn.ServerVersion())
			t.Log(conn.Ping(context.Background()))
		}
	}
}
func TestPingDeadline(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//Debug: true,
	})
	if assert.NoError(t, err) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if err := conn.Ping(ctx); assert.Error(t, err) {
			assert.Equal(t, err, context.DeadlineExceeded)
		}
	}
}
