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
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdContextStdTimeout(t *testing.T) {
	dsns := map[string]string{"Native": "clickhouse://127.0.0.1:9000", "Http": "http://127.0.0.1:8123"}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			if connect, err := sql.Open("clickhouse", dsn); assert.NoError(t, err) && assert.NoError(t, connect.Ping()) {
				{
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*20)
					defer cancel()
					if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(1)"); assert.NotNil(t, row) {
						var a, b int
						if err := row.Scan(&a, &b); assert.Error(t, err) {
							switch err := err.(type) {
							case *net.OpError:
								assert.Equal(t, "read", err.Op)
							case *url.Error:
								assert.Equal(t, context.DeadlineExceeded, err.Err)
							default:
								assert.Equal(t, context.DeadlineExceeded, err)
							}
						}
					}
				}
				{
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if row := connect.QueryRowContext(ctx, "SELECT 1, sleep(0.1)"); assert.NotNil(t, row) {
						var value, value2 int
						if assert.NoError(t, row.Scan(&value, &value2)) {
							assert.Equal(t, int(1), value)
						}
					}
				}
			}
		})
	}
}
