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

package http

import (
	"context"
	"database/sql"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHttpConn(t *testing.T) {
	if conn, err := sql.Open("clickhousehttp", "http://127.0.0.1:8123?dial_timeout=1s&compress=true"); assert.NoError(t, err) {
		if assert.NoError(t, err) {
			if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
				if assert.NoError(t, conn.Close()) {
					t.Log(conn.Stats())
				}
			}
		}
	}
}
