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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func TestQueryParameters(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	connectionString := fmt.Sprintf("http://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60", env.Host, env.HttpPort, env.Username, env.Password)
	if useSSL {
		connectionString = fmt.Sprintf("https://%s:%d?username=%s&password=%s&dial_timeout=200ms&max_execution_time=60&secure=true", env.Host, env.HttpsPort, env.Username, env.Password)
	}
	dsns := map[string]string{"Http": connectionString}

	for name, dsn := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetConnectionFromDSN(dsn)
			require.NoError(t, err)

			t.Run("with named arguments", func(t *testing.T) {
				var actualNum uint64
				var actualStr string
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					clickhouse.Named("num", "42"),
					clickhouse.Named("str", "hello"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum, &actualStr))

				assert.Equal(t, uint64(42), actualNum)
				assert.Equal(t, "hello", actualStr)
			})

			t.Run("named args with only strings supported", func(t *testing.T) {
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					clickhouse.Named("num", 42),
					clickhouse.Named("str", "hello"),
				)
				require.ErrorIs(t, row.Err(), clickhouse.ErrExpectedStringValueInNamedValueForQueryParameter)
			})

			t.Run("with identifier type", func(t *testing.T) {
				var actualNum uint64

				row := conn.QueryRow(
					"SELECT {column:Identifier} FROM {database:Identifier}.{table:Identifier} LIMIT 1 OFFSET 100;",
					clickhouse.Named("column", "number"),
					clickhouse.Named("database", "system"),
					clickhouse.Named("table", "numbers"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum))

				assert.Equal(t, uint64(100), actualNum)
			})

			t.Run("unsupported arg type", func(t *testing.T) {
				row := conn.QueryRow(
					"SELECT {num:UInt64}, {str:String}",
					1234,
					"String",
				)
				require.ErrorIs(t, row.Err(), clickhouse.ErrExpectedStringValueInNamedValueForQueryParameter)
			})

			t.Run("with bind backwards compatibility", func(t *testing.T) {
				var actualNum uint8
				var actualStr string
				row := conn.QueryRow(
					"SELECT @num, @str",
					clickhouse.Named("num", 42),
					clickhouse.Named("str", "hello"),
				)
				require.NoError(t, row.Err())
				require.NoError(t, row.Scan(&actualNum, &actualStr))

				assert.Equal(t, uint8(42), actualNum)
				assert.Equal(t, "hello", actualStr)
			})
		})
	}
}
