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
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleNested(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			//	Debug: true,
		})
	)
	require.NoError(t, err)
	if err := checkMinServerVersion(conn, 22, 1, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 String
				)
			) Engine Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []string{"1", "2", "3"}
	)
	require.NoError(t, batch.Append(col1Data))
	require.NoError(t, batch.Send())
	var (
		col1 []string
	)
	if err := conn.QueryRow(ctx, "SELECT * FROM test_nested").Scan(&col1); assert.NoError(t, err) {
		assert.Equal(t, col1Data, col1)
	}
}

// this isn't documented behaviour in ClickHouse - i.e. flatten_nested=1 with multiple Nested. Following does work however.
func TestNestedFlattened(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Settings: clickhouse.Settings{
				"flatten_nested": 1,
			},
		})
	)
	require.NoError(t, err)
	if err := checkMinServerVersion(conn, 22, 1, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_nested")
	}()
	if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
		if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested"); assert.NoError(t, err) {
			fmt.Println(batch)
			var (
				col1Data = []uint8{1, 2, 3}

				col2Data = []uint8{10, 20, 30}
				col3Data = []uint8{101, 201, 230} // Col2.Col1_N2
				col4Data = [][][]interface{}{
					[][]interface{}{
						[]interface{}{uint8(1), uint8(2)},
					},
					[][]interface{}{
						[]interface{}{uint8(1), uint8(2)},
					},
					[][]interface{}{
						[]interface{}{uint8(1), uint8(2)},
					},
				}
			)
			require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data))
			require.NoError(t, batch.Send())
			var (
				col1 []uint8
				col2 []uint8
				col3 []uint8
				col4 [][][]interface{}
			)
			rows := conn.QueryRow(ctx, "SELECT * FROM test_nested")
			require.NoError(t, rows.Scan(&col1, &col2, &col3, &col4))
			assert.Equal(t, col1Data, col1)
			assert.Equal(t, col2Data, col2)
			assert.Equal(t, col3Data, col3)
			assert.Equal(t, col4Data, col4)
		}
	}

}

func TestFlattenedSimpleNested(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Settings: clickhouse.Settings{
				"flatten_nested": 0,
			},
		})
	)
	require.NoError(t, err)
	if err := checkMinServerVersion(conn, 22, 1, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 String
				)
			) Engine Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_nested")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested")
	require.NoError(t, err)
	var (
		col1Data = []map[string]interface{}{
			{
				"Col1_N1": "1",
			},
			{
				"Col1_N1": "2",
			},
			{
				"Col1_N1": "3",
			},
		}
	)
	require.NoError(t, batch.Append(col1Data))
	require.NoError(t, batch.Send())
	var (
		col1 []map[string]interface{}
	)
	if err := conn.QueryRow(ctx, "SELECT * FROM test_nested").Scan(&col1); assert.NoError(t, err) {
		assert.Equal(t, col1Data, col1)
	}
}

// nested with flatten_nested = 0
func TestNestedUnFlattened(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			Settings: clickhouse.Settings{
				"flatten_nested": 0,
			},
		})
	)
	if assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 22, 1, 0); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TABLE test_nested (
				Col1 Nested(
					  Col1_N1 UInt8
					, Col2_N1 UInt8
				)
				, Col2 Nested(
					  Col1_N2 UInt8
					, Col2_N2 Nested(
						  Col1_N2_N1 UInt8
						, Col2_N2_N1 UInt8
					)
				)
			) Engine Memory
		`
		defer func() {
			conn.Exec(ctx, "DROP TABLE test_nested")
		}()
		if err := conn.Exec(ctx, ddl); assert.NoError(t, err) {
			if batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nested"); assert.NoError(t, err) {
				fmt.Println(batch)
				var (
					col1Data = []map[string]interface{}{
						{
							"Col1_N1": uint8(1),
							"Col2_N1": uint8(20),
						},
						{
							"Col1_N1": uint8(2),
							"Col2_N1": uint8(20),
						},
						{
							"Col1_N1": uint8(3),
							"Col2_N1": uint8(20),
						},
					}
					col2Data = []map[string]interface{}{
						{
							"Col1_N2": uint8(101),
							"Col2_N2": []map[string]interface{}{
								{
									"Col1_N2_N1": uint8(1),
									"Col2_N2_N1": uint8(2),
								},
							},
						},
						{
							"Col1_N2": uint8(201),
							"Col2_N2": []map[string]interface{}{
								{
									"Col1_N2_N1": uint8(3),
									"Col2_N2_N1": uint8(4),
								},
							},
						},
					}
				)
				require.NoError(t, batch.Append(col1Data, col2Data))
				require.NoError(t, batch.Send())
				var (
					col1 []map[string]interface{}
					col2 []map[string]interface{}
				)
				rows := conn.QueryRow(ctx, "SELECT * FROM test_nested")
				require.NoError(t, rows.Scan(&col1, &col2))
				assert.Equal(t, col1Data, col1)
				assert.Equal(t, col2Data, col2)
			}
		}
	}
}
