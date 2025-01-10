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
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

var jsonTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupJSONTest(t *testing.T) driver.Conn {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"max_execution_time":           60,
		"allow_experimental_json_type": true,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerServerVersion(conn, 24, 9, 0) {
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

	jsonRow := chcol.NewJSON()
	jsonRow.SetValueAtPath("Name", "JSON")
	jsonRow.SetValueAtPath("Age", int64(42))
	jsonRow.SetValueAtPath("Active", true)
	jsonRow.SetValueAtPath("Score", 3.14)
	jsonRow.SetValueAtPath("Tags", []string{"a", "b"})
	jsonRow.SetValueAtPath("Numbers", []int64{20, 40})
	jsonRow.SetValueAtPath("Address.Street", "Street")
	jsonRow.SetValueAtPath("Address.City", "City")
	jsonRow.SetValueAtPath("Address.Country", "Country")
	jsonRow.SetValueAtPath("KeysNumbers", map[string]int64{"FieldA": 42, "FieldB": 32})
	jsonRow.SetValueAtPath("Metadata.FieldA", "a")
	jsonRow.SetValueAtPath("Metadata.FieldB", "b")
	jsonRow.SetValueAtPath("Metadata.FieldC.FieldD", "d")
	jsonRow.SetValueAtPath("Timestamp", jsonTestDate)
	jsonRow.SetValueAtPath("DynamicString", clickhouse.NewDynamic("str"))
	jsonRow.SetValueAtPath("DynamicInt", clickhouse.NewDynamic(int64(48)))
	jsonRow.SetValueAtPath("DynamicMap", clickhouse.NewDynamic(map[string]string{"a": "a", "b": "b"}))

	require.NoError(t, batch.Append(jsonRow))
	require.NoError(t, batch.Send())

	rows, err := conn.Query(ctx, "SELECT c FROM test_json")
	require.NoError(t, err)

	var row chcol.JSON

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

type Address struct {
	Street  string `chType:"String"`
	City    string `chType:"String"`
	Country string `chType:"String"`
}

type TestStruct struct {
	Name   string
	Age    int64
	Active bool
	Score  float64

	Tags    []string
	Numbers []int64

	Address Address

	KeysNumbers map[string]int64
	Metadata    map[string]interface{}

	Timestamp time.Time `chType:"DateTime64(3)"`

	DynamicString chcol.Dynamic
	DynamicInt    chcol.Dynamic
	DynamicMap    chcol.Dynamic
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

	inputRow := TestStruct{
		Name:    "JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: Address{
			Street:  "Street",
			City:    "City",
			Country: "Country",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": "d",
			},
		},
		Timestamp:     jsonTestDate,
		DynamicString: chcol.NewDynamic("str").WithType("String"),
		DynamicInt:    chcol.NewDynamic(int64(48)).WithType("Int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
	}
	require.NoError(t, batch.Append(inputRow))

	inputRow2 := TestStruct{
		KeysNumbers: map[string]int64{},
		Timestamp:   jsonTestDate,
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

	inputRow := TestStruct{
		Name:    "JSON",
		Age:     42,
		Active:  true,
		Score:   3.14,
		Tags:    []string{"a", "b"},
		Numbers: []int64{20, 40},
		Address: Address{
			Street:  "Street",
			City:    "City",
			Country: "Country",
		},
		KeysNumbers: map[string]int64{"FieldA": 42, "FieldB": 32},
		Metadata: map[string]interface{}{
			"FieldA": "a",
			"FieldB": "b",
			"FieldC": map[string]interface{}{
				"FieldD": "d",
			},
		},
		Timestamp:     jsonTestDate,
		DynamicString: chcol.NewDynamic("str").WithType("String"),
		DynamicInt:    chcol.NewDynamic(int64(48)).WithType("Int64"),
		DynamicMap:    chcol.NewDynamic(map[string]string{"a": "a", "b": "b"}).WithType("Map(String, String)"),
	}

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
