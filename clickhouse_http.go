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

package clickhouse

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2/contributors"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type clickhouseHTTP struct {
	conn *httpConnect
}

func (c *clickhouseHTTP) Contributors() []string {
	list := contributors.List
	if len(list[len(list)-1]) == 0 {
		return list[:len(list)-1]
	}

	return list
}

func (c *clickhouseHTTP) ServerVersion() (*driver.ServerVersion, error) {
	return &c.conn.handshake, nil
}

func (c *clickhouseHTTP) Select(ctx context.Context, dest any, query string, args ...any) error {
	return scanSelect(c.Query, ctx, dest, query, args...)
}

func (c *clickhouseHTTP) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.conn.query(ctx, nil, query, args...)
}

func (c *clickhouseHTTP) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	return c.conn.queryRow(ctx, nil, query, args...)
}

func (c *clickhouseHTTP) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	return c.conn.prepareBatch(ctx, query, getPrepareBatchOptions(opts...), nil, nil)
}

func (c *clickhouseHTTP) Exec(ctx context.Context, query string, args ...any) error {
	return c.conn.exec(ctx, query, args...)
}

func (c *clickhouseHTTP) AsyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return c.conn.asyncInsert(ctx, query, wait, args...)
}

func (c *clickhouseHTTP) Ping(ctx context.Context) error {
	return c.conn.ping(ctx)
}

func (c *clickhouseHTTP) Stats() driver.Stats {
	//TODO: proper implementation
	return driver.Stats{
		MaxOpenConns: 1,
		MaxIdleConns: 1,
		Open:         1,
		Idle:         0,
	}
}

func (c *clickhouseHTTP) Close() error {
	return c.conn.close()
}
