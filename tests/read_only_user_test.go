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
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestReadOnlyUser(t *testing.T) {
	readQueryCases := []struct {
		name        string
		query       string
		assertRowFn func(row driver.Row)
	}{
		{
			name:  "select from table",
			query: "SELECT sum(Col1) FROM test_readonly_user",
			assertRowFn: func(row driver.Row) {
				var expectedValue uint64
				err := row.Scan(&expectedValue)
				assert.NoError(t, err)

				assert.Equal(t, expectedValue, uint64(5))
			},
		},
	}

	writeQueryCases := []struct {
		name  string
		query string
	}{
		{
			name:  "create table",
			query: "CREATE TABLE some_table (Col1 UInt8) Engine MergeTree() ORDER BY tuple()",
		},
		{
			name:  "insert into table",
			query: "INSERT INTO test_readonly_user VALUES (0)"},
		{
			name:  "drop table",
			query: "DROP TABLE test_readonly_user",
		},
	}

	setSettingQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "set setting",
			query: "SET log_queries = 0",
		},
	}

	ctx := context.Background()

	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	client, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	defer client.Close()

	require.NoError(t, createSimpleTable(client, "test_readonly_user"))
	defer dropTable(client, "test_readonly_user")
	require.NoError(t, client.Exec(ctx, `
		INSERT INTO test_readonly_user VALUES (5)
	`))

	username, password, err := createUserWithReadOnlySetting(client, env.Database, readOnlyRead)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, dropUser(client, username))
	}()

	roEnv := env
	roEnv.Username = username
	roEnv.Password = password

	roClient, err := testClientWithDefaultOptions(roEnv, nil)
	require.NoError(t, err)
	defer roClient.Close()

	for _, testCase := range readQueryCases {
		t.Run(testCase.name, func(t *testing.T) {
			row := roClient.QueryRow(ctx, testCase.query)
			assert.NoError(t, err)
			testCase.assertRowFn(row)
		})
	}

	for _, testCase := range writeQueryCases {
		t.Run(testCase.name, func(t *testing.T) {
			actualErr := roClient.Exec(ctx, testCase.query)

			assert.ErrorContains(t, actualErr, "Cannot execute query in readonly mode")
		})
	}

	for _, testCase := range setSettingQueries {
		t.Run(testCase.name, func(t *testing.T) {
			actualErr := roClient.Exec(ctx, testCase.query)

			assert.ErrorContains(t, actualErr, "setting in readonly mode")
		})
	}
}
