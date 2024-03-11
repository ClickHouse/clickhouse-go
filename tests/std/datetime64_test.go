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
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStdDateTime64(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			if !CheckMinServerVersion(conn, 20, 3, 0) {
				t.Skip(fmt.Errorf("unsupported clickhouse version"))
				return
			}
			const ddl = `
			CREATE TABLE test_datetime64 (
				  Col1 DateTime64(3)
				, Col2 DateTime64(9, 'Europe/Moscow')
				, Col3 DateTime64(0, 'Europe/London')
				, Col4 Nullable(DateTime64(3, 'Europe/Moscow'))
				, Col5 Array(DateTime64(3, 'Europe/Moscow'))
				, Col6 Array(Nullable(DateTime64(3, 'Europe/Moscow')))
				, Col7 DateTime64(0, 'Europe/London')
				, Col8 Nullable(DateTime64(3, 'Europe/Moscow'))
				, Col9 DateTime64(9)
				, Col10 DateTime64(9)
			) Engine MergeTree() ORDER BY tuple()
		`
			defer func() {
				conn.Exec("DROP TABLE test_datetime64")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_datetime64")
			require.NoError(t, err)
			var (
				datetime1 = time.Now().Truncate(time.Millisecond)
				datetime2 = time.Now().Truncate(time.Nanosecond)
				datetime3 = time.Now().Truncate(time.Second)
			)
			expectedMinDateTime, err := time.Parse("2006-01-02 15:04:05", "1900-01-01 00:00:00")
			require.NoError(t, err)
			_, err = batch.Exec(
				datetime1,
				datetime2,
				datetime3,
				&datetime1,
				[]time.Time{datetime1, datetime1},
				[]*time.Time{&datetime3, nil, &datetime3},
				sql.NullTime{Time: datetime3, Valid: true},
				sql.NullTime{Time: time.Time{}, Valid: false},
				expectedMinDateTime,
				time.Time{},
			)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1  time.Time
				col2  time.Time
				col3  time.Time
				col4  *time.Time
				col5  []time.Time
				col6  []*time.Time
				col7  sql.NullTime
				col8  sql.NullTime
				col9  time.Time
				col10 time.Time
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_datetime64").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9, &col10))
			assert.Equal(t, datetime1.In(time.UTC), col1)
			assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
			assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
			require.Equal(t, "Europe/Moscow", col2.Location().String())
			assert.Equal(t, "Europe/London", col3.Location().String())

			assert.Equal(t, datetime1.UnixNano(), col4.UnixNano())
			require.Len(t, col5, 2)
			assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
			assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
			require.Len(t, col6, 3)
			assert.Nil(t, col6[1])
			assert.NotNil(t, col6[0])
			assert.NotNil(t, col6[2])
			require.Equal(t, sql.NullTime{Time: datetime3.In(col7.Time.Location()), Valid: true}, col7)
			require.Equal(t, sql.NullTime{Time: time.Time{}, Valid: false}, col8)
			require.Equal(t, time.Date(1900, 01, 01, 0, 0, 0, 0, time.UTC), col9)
			require.Equal(t, time.Unix(0, 0).UTC(), col10)
		})
	}
}
