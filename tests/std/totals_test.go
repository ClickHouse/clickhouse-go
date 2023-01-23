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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStdWithTotals(t *testing.T) {
	const query = `
	SELECT
		number AS n
		, COUNT()
	FROM (
		SELECT number FROM system.numbers LIMIT 100
	) GROUP BY n WITH TOTALS
	`
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := GetStdDSNConnection(clickhouse.Native, useSSL, nil)
	require.NoError(t, err)
	rows, err := conn.Query(query)
	require.NoError(t, err)
	var count int
	for rows.Next() {
		count++
		var (
			n uint64
			c uint64
		)
		require.NoError(t, rows.Scan(&n, &c))
	}
	require.Equal(t, 100, count)
	require.True(t, rows.NextResultSet())
	count = 0
	for rows.Next() {
		count++
		var (
			n, totals uint64
		)
		require.NoError(t, rows.Scan(&n, &totals))
		assert.Equal(t, uint64(0), n)
		assert.Equal(t, uint64(100), totals)
	}
	assert.Equal(t, 1, count)
}
