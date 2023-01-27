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
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssue357(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	require.NoError(t, err)

	const ddl = ` -- foo.bar DDL comment
		CREATE TEMPORARY TABLE issue_357 (
			  Col1 Int32
			, Col2 DateTime
		)
		`
	defer func() {
		conn.Exec("DROP TABLE issue_357")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)
	scope, err := conn.Begin()
	require.NoError(t, err)
	const query = ` -- foo.bar Insert comment
				INSERT INTO issue_357
				`
	batch, err := scope.Prepare(query)

	require.NoError(t, err)
	_, err = batch.Exec(int32(42), time.Now())
	require.NoError(t, err)
	require.NoError(t, scope.Commit())
	var (
		col1 int32
		col2 time.Time
	)
	require.NoError(t, conn.QueryRow("SELECT * FROM issue_357").Scan(&col1, &col2))
	assert.Equal(t, int32(42), col1)
}
