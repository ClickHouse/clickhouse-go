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

func TestStdDateTime(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, nil)
			require.NoError(t, err)
			const ddl = `
			CREATE TABLE test_datetime (
				  Col1 DateTime
				, Col2 DateTime('Europe/Moscow')
				, Col3 DateTime('Europe/London')
				, Col4 Nullable(DateTime('Europe/Moscow'))
				, Col5 Array(DateTime('Europe/Moscow'))
				, Col6 Array(Nullable(DateTime('Europe/Moscow')))
			    , Col7 DateTime
			    , Col8 Nullable(DateTime)
				,Col9 DateTime
			) Engine MergeTree() ORDER BY tuple()`
			defer func() {
				conn.Exec("DROP TABLE test_datetime")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_datetime")
			require.NoError(t, err)
			datetime := time.Now().Truncate(time.Second)
			_, err = batch.Exec(
				datetime,
				datetime,
				datetime,
				&datetime,
				[]time.Time{datetime, datetime},
				[]*time.Time{&datetime, nil, &datetime},
				sql.NullTime{
					Time:  datetime,
					Valid: true,
				},
				sql.NullTime{
					Time:  time.Time{},
					Valid: false,
				},
				time.Time{},
			)
			require.NoError(t, err)
			require.NoError(t, scope.Commit())
			var (
				col1 time.Time
				col2 time.Time
				col3 time.Time
				col4 *time.Time
				col5 []time.Time
				col6 []*time.Time
				col7 sql.NullTime
				col8 sql.NullTime
				col9 time.Time
			)
			require.NoError(t, conn.QueryRow("SELECT * FROM test_datetime").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8, &col9))
			assert.Equal(t, datetime.In(time.UTC), col1)
			assert.Equal(t, datetime.Unix(), col2.Unix())
			assert.Equal(t, datetime.Unix(), col3.Unix())
			if name == "Http" {
				// Native Format over HTTP works with revision 0
				// Clickhouse before 54337 revision don't support Time Zone
				// So, it removes Time Zone if revision less than 54337
				// https://github.com/ClickHouse/ClickHouse/issues/38209
				// pending https://github.com/ClickHouse/ClickHouse/issues/40397
				require.Equal(t, "UTC", col2.Location().String())
				require.Equal(t, "UTC", col3.Location().String())
			} else {
				if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
					assert.Equal(t, "Europe/London", col3.Location().String())
				}
			}
			assert.Equal(t, datetime.Unix(), col4.Unix())
			require.Len(t, col5, 2)
			assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
			assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
			require.Len(t, col6, 3)
			assert.Nil(t, col6[1])
			assert.NotNil(t, col6[0])
			assert.NotNil(t, col6[2])
			assert.Equal(t, sql.NullTime{
				Time:  datetime.In(time.UTC),
				Valid: true,
			}, col7)
			assert.Equal(t, sql.NullTime{
				Time:  time.Time{},
				Valid: false,
			}, col8)
			assert.Equal(t, time.Unix(0, 0).UTC(), col9)
		})
	}
}
