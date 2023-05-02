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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test759(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	timeWant, err := time.Parse(time.RFC3339Nano, "2022-09-15T17:06:31.81718722+04:00")
	require.NoError(t, err)
	testWith(t, conn, timeWant.Local())
	testWith(t, conn, timeWant)

}

func testWith(t *testing.T, conn driver.Conn, timeWant time.Time) {
	date := clickhouse.DateNamed("Time", timeWant, clickhouse.NanoSeconds)
	r := conn.QueryRow(context.TODO(), "SELECT @Time", date)

	var timeGot time.Time
	require.NoError(t, r.Scan(&timeGot))
	require.Equal(t, timeGot.Unix(), timeWant.Unix())
}
