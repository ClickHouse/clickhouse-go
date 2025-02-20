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
	"encoding/json"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
	"testing"
)

func setupJSONTest(t *testing.T) driver.Conn {
	SkipOnCloud(t, "cannot modify JSON settings on cloud")

	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":              60,
		"allow_experimental_variant_type": true,
		"allow_experimental_dynamic_type": true,
		"allow_experimental_json_type":    true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerServerVersion(conn, 24, 10, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version for JSON type"))
		return nil
	}

	return conn
}

func TestJSONPaths(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	jsonRow := BuildTestJSONPaths()

	require.NoError(t, batch.Append(jsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row clickhouse.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	expectedValuesByPath := jsonRow.ValuesByPath()
	actualValuesByPath := row.ValuesByPath()
	for path, expectedValue := range expectedValuesByPath {
		actualValue, ok := actualValuesByPath[path]
		if !ok {
			t.Fatalf("result JSON is missing path: %s", path)
		}

		// Allow Equal func to compare values without Dynamic wrapper
		if v, ok := expectedValue.(clickhouse.Dynamic); ok {
			expectedValue = v.Any()
		}

		if v, ok := actualValue.(clickhouse.Dynamic); ok {
			actualValue = v.Any()
		}

		require.Equal(t, expectedValue, actualValue)
	}
}

func TestJSONArray(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c Array(JSON)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	arrJsonRow := []*clickhouse.JSON{clickhouse.NewJSON(), BuildTestJSONPaths()}

	require.NoError(t, batch.Append(arrJsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var arrRow []*clickhouse.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 2)

	actualValuesByPathEmpty := arrRow[0].ValuesByPath()
	for _, actualValue := range actualValuesByPathEmpty {
		// Allow Nil func to compare values without Dynamic wrapper
		if v, ok := actualValue.(clickhouse.Dynamic); ok {
			actualValue = v.Any()
		}

		require.Nil(t, actualValue)
	}

	expectedValuesByPath := arrJsonRow[1].ValuesByPath()
	actualValuesByPath := arrRow[1].ValuesByPath()
	for path, expectedValue := range expectedValuesByPath {
		actualValue, ok := actualValuesByPath[path]
		if !ok {
			t.Fatalf("result JSON is missing path: %s", path)
		}

		// Allow Equal func to compare values without Dynamic wrapper
		if v, ok := expectedValue.(clickhouse.Dynamic); ok {
			expectedValue = v.Any()
		}

		if v, ok := actualValue.(clickhouse.Dynamic); ok {
			actualValue = v.Any()
		}

		require.Equal(t, expectedValue, actualValue)
	}
}

func TestJSONEmptyArray(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c Array(JSON)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	var arrJsonRow []*clickhouse.JSON
	require.NoError(t, batch.Append(arrJsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var arrRow []*clickhouse.JSON

	require.True(t, rows.Next())
	err = rows.Scan(&arrRow)
	require.NoError(t, err)
	require.Len(t, arrRow, 0)
}

func TestJSONStruct(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildTestJSONStruct()
	require.NoError(t, batch.Append(inputRow))

	inputRow2 := TestStruct{
		KeysNumbers: map[string]int64{},
		Timestamp:   JSONTestDate,
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": int64(5),
			},
			"FieldE": map[string]interface{}{
				"FieldF": "f",
			},
		},
	}
	require.NoError(t, batch.Append(inputRow2))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	// The second row adds a nil value at this path. Update the inputRow for easier deep equal check
	inputRow.Metadata["FieldE"] = map[string]interface{}{
		"FieldF": nil,
	}
	require.Equal(t, inputRow, row)

	var row2 TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row2)
	require.NoError(t, err)
	// Init slices for easier comparison
	inputRow2.Tags = make([]string, 0)
	inputRow2.Numbers = make([]int64, 0)
	require.Equal(t, inputRow2, row2)
}

func TestJSONFastStruct(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildFastTestJSONStruct()
	require.NoError(t, batch.Append(&inputRow))

	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row TestStruct

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)
	require.Equal(t, inputRow.ts, row)
}

func TestJSONString(t *testing.T) {
	t.Skip("client cannot receive JSON strings")

	ctx := context.Background()
	conn := setupJSONTest(t)

	require.NoError(t, conn.Exec(ctx, "SET output_format_native_write_json_as_string=1"))

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	inputRow := BuildTestJSONStruct()

	inputRowStr, err := json.Marshal(inputRow)
	require.NoError(t, err)
	require.NoError(t, batch.Append(inputRowStr))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row json.RawMessage

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	require.Equal(t, string(inputRowStr), string(row))

	var rowStruct TestStruct
	err = json.Unmarshal(row, &rowStruct)
	require.NoError(t, err)
}

func TestJSON_BatchFlush(t *testing.T) {
	t.Skip(fmt.Errorf("server-side JSON bug"))

	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS test_json (
				  c JSON
			) Engine = MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		require.NoError(t, conn.Exec(ctx, "DROP TABLE IF EXISTS test_json"))
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_json (c)")
	require.NoError(t, err)

	vals := make([]*clickhouse.JSON, 0, 1000)
	for i := 0; i < 1000; i++ {
		row := clickhouse.NewJSON()
		if i%2 == 0 {
			row.SetValueAtPath("a", int64(i))
			row.SetValueAtPath("b", i%5 == 0)
		} else {
			row.SetValueAtPath("c", int64(-i))
			row.SetValueAtPath("d", i%5 != 0)
		}

		vals = append(vals, row)
		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	i := 0
	for rows.Next() {
		var row clickhouse.JSON
		err = rows.Scan(&row)
		require.NoError(t, err)

		if i%2 == 0 {
			valA, ok := row.ValueAtPath("a")
			require.Equal(t, true, ok)
			_, ok = valA.(clickhouse.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, int64(i), valA.(clickhouse.Dynamic).Any())
			require.Equal(t, "Int64", valA.(clickhouse.Dynamic).Type())

			valB, ok := row.ValueAtPath("b")
			require.Equal(t, true, ok)
			_, ok = valB.(clickhouse.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, i%5 == 0, valB.(clickhouse.Dynamic).Any())
			require.Equal(t, "Bool", valB.(clickhouse.Dynamic).Type())
		} else {
			valC, ok := row.ValueAtPath("c")
			require.Equal(t, true, ok)
			_, ok = valC.(clickhouse.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, int64(-i), valC.(clickhouse.Dynamic).Any())
			require.Equal(t, "Int64", valC.(clickhouse.Dynamic).Type())

			valD, ok := row.ValueAtPath("d")
			require.Equal(t, true, ok)
			_, ok = valD.(clickhouse.Dynamic)
			require.Equal(t, true, ok)

			require.Equal(t, i%5 != 0, valD.(clickhouse.Dynamic).Any())
			require.Equal(t, "Bool", valD.(clickhouse.Dynamic).Type())
		}

		i++
	}
}

// https://github.com/grafana/clickhouse-datasource/issues/1168
func TestJSONArrayDynamic(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::Array(JSON)::Dynamic AS c`)
	require.NoError(t, err)

	require.True(t, rows.Next())
}

func TestJSONArrayVariant(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	rows, err := conn.Query(ctx, `SELECT ['{"x":5}','{"y":6}']::Array(JSON)::Variant(Array(JSON)) AS c`)
	require.NoError(t, err)

	require.True(t, rows.Next())
}
