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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestQueryParameters(t *testing.T) {
	ctx := context.Background()

	env, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	client, err := TestClientWithDefaultSettings(env)
	require.NoError(t, err)
	defer client.Close()

	if !CheckMinServerServerVersion(client, 22, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}

	t.Run("with context parameters", func(t *testing.T) {
		chCtx := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{
			"num":   "42",
			"str":   "hello",
			"array": "['a', 'b', 'c']",
		}))

		var actualNum uint64
		var actualStr string
		var actualArray []string
		row := client.QueryRow(chCtx, "SELECT {num:UInt64} v, {str:String} s, {array:Array(String)} a")
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr, &actualArray))

		assert.Equal(t, uint64(42), actualNum)
		assert.Equal(t, "hello", actualStr)
		assert.Equal(t, []string{"a", "b", "c"}, actualArray)
	})

	t.Run("with named arguments", func(t *testing.T) {
		var actualNum uint64
		var actualStr string
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			clickhouse.Named("num", "42"),
			clickhouse.Named("str", "hello"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr))

		assert.Equal(t, uint64(42), actualNum)
		assert.Equal(t, "hello", actualStr)
	})

	t.Run("with identifier type", func(t *testing.T) {
		var actualNum uint64

		row := client.QueryRow(
			ctx,
			"SELECT {column:Identifier} FROM {database:Identifier}.{table:Identifier} LIMIT 1 OFFSET 100;",
			clickhouse.Named("column", "number"),
			clickhouse.Named("database", "system"),
			clickhouse.Named("table", "numbers"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum))

		assert.Equal(t, uint64(100), actualNum)
	})

	t.Run("named args with only strings supported", func(t *testing.T) {
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			clickhouse.Named("num", 42),
			clickhouse.Named("str", "hello"),
		)
		require.ErrorIs(t, row.Err(), clickhouse.ErrExpectedStringValueInNamedValueForQueryParameter)
	})

	t.Run("unsupported arg type", func(t *testing.T) {
		row := client.QueryRow(
			ctx,
			"SELECT {num:UInt64}, {str:String}",
			1234,
			"String",
		)
		require.ErrorIs(t, row.Err(), clickhouse.ErrExpectedStringValueInNamedValueForQueryParameter)
	})

	t.Run("with bind backwards compatibility", func(t *testing.T) {
		var actualNum uint8
		var actualStr string
		row := client.QueryRow(
			ctx,
			"SELECT @num, @str",
			clickhouse.Named("num", 42),
			clickhouse.Named("str", "hello"),
		)
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&actualNum, &actualStr))

		assert.Equal(t, uint8(42), actualNum)
		assert.Equal(t, "hello", actualStr)
	})
}
