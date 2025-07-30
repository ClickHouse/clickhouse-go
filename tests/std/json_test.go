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
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/chcol"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

var jsonTestDate, _ = time.Parse(time.RFC3339, "2024-12-13T02:09:30.123Z")

func setupJSONTest(t *testing.T) *sql.DB {
	clickhouse_tests.SkipOnCloud(t, "cannot modify JSON settings on cloud")

	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)

	if !CheckMinServerVersion(conn, 24, 8, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version for JSON type"))
		return nil
	}

	_, err = conn.ExecContext(context.Background(), "SET allow_experimental_json_type = 1")
	if err != nil {
		t.Fatal(err)
		return nil
	}

	if CheckMinServerVersion(conn, 25, 6, 0) {
		_, err = conn.ExecContext(context.Background(), "SET output_format_native_use_flattened_dynamic_and_json_serialization = 1")
		if err != nil {
			t.Fatal(err)
			return nil
		}
	}

	return conn
}

func TestJSONPaths(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS std_test_json_paths (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS std_test_json_paths")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO std_test_json_paths (c)")
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

	_, err = batch.ExecContext(ctx, jsonRow)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM std_test_json_paths")
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

	require.NoError(t, rows.Close())
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
	t.Skip("scan skips struct reflection")

	ctx := context.Background()
	conn := setupJSONTest(t)

	const ddl = `
			CREATE TABLE IF NOT EXISTS std_test_json_struct (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	_, err := conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS std_test_json_struct")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO std_test_json_struct (c)")
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
	_, err = batch.ExecContext(ctx, inputRow)
	require.NoError(t, err)

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
	_, err = batch.ExecContext(ctx, inputRow2)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM std_test_json_struct")
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

	require.NoError(t, rows.Close())
}

func TestJSONString(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	if !CheckMinServerVersion(conn, 24, 10, 0) {
		t.Skip("JSON strings not supported")
	}

	_, err := conn.ExecContext(ctx, "SET output_format_native_write_json_as_string = 1")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET output_format_json_quote_64bit_integers = 0")
	require.NoError(t, err)

	const ddl = `
			CREATE TABLE IF NOT EXISTS std_test_json_string (
				  c JSON(Name String, Age Int64, KeysNumbers Map(String, Int64), SKIP fake.field)
			) Engine = MergeTree() ORDER BY tuple()
		`
	_, err = conn.ExecContext(ctx, ddl)
	require.NoError(t, err)
	defer func() {
		_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS std_test_json_string")
		require.NoError(t, err)
	}()

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)

	batch, err := tx.PrepareContext(ctx, "INSERT INTO std_test_json_string (c)")
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

	_, err = batch.ExecContext(ctx, inputRowStr)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	rows, err := conn.QueryContext(ctx, "SELECT c FROM std_test_json_string")
	require.NoError(t, err)

	var row string

	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	// ClickHouse server will sort JSON fields differently.
	// In order to do a proper string comparison, we unmarshal and remarshal
	// to our test struct to get Go's ordering of the fields.
	var rowStruct TestStruct
	err = json.Unmarshal([]byte(row), &rowStruct)
	require.NoError(t, err)

	remarshalBytes, err := json.Marshal(rowStruct)
	require.NoError(t, err)

	require.Equal(t, inputRowStr, remarshalBytes)

	require.NoError(t, rows.Close())
}

func TestJSONStringScanTypes(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	if !CheckMinServerVersion(conn, 24, 10, 0) {
		t.Skip("JSON strings not supported")
	}

	_, err := conn.ExecContext(ctx, "SET output_format_native_write_json_as_string = 1")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET output_format_json_quote_64bit_integers = 0")
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT arrayJoin(['{\"x\": 1}', '{\"x\": 2}', '{\"x\": 3}']::Array(JSON))")
	require.NoError(t, err)

	var rowStr string
	require.True(t, rows.Next())
	err = rows.Scan(&rowStr)
	require.NoError(t, err)
	require.Equal(t, "{\"x\":1}", rowStr)

	var rowBytes []byte
	require.True(t, rows.Next())
	err = rows.Scan(&rowBytes)
	require.NoError(t, err)
	require.Equal(t, []byte("{\"x\":2}"), rowBytes)

	var rowJSONRawMessage json.RawMessage
	require.True(t, rows.Next())
	err = rows.Scan(&rowJSONRawMessage)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage("{\"x\":3}"), rowJSONRawMessage)

	require.NoError(t, rows.Close())
}

func TestJSONNullableObjectScan(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	if !CheckMinServerVersion(conn, 25, 2, 0) {
		t.Skip("Nullable(JSON) unsupported")
	}

	rows, err := conn.QueryContext(ctx, "SELECT '{\"x\": 1}'::Nullable(JSON)")
	require.NoError(t, err)

	var row clickhouse.JSON
	require.True(t, rows.Next())
	err = rows.Scan(&row)
	require.NoError(t, err)

	val, ok := clickhouse.ExtractJSONPathAs[int64](&row, "x")
	require.True(t, ok)
	require.Equal(t, int64(1), val)

	require.NoError(t, rows.Close())
}

func TestJSONNullableStringScan(t *testing.T) {
	ctx := context.Background()
	conn := setupJSONTest(t)

	if !CheckMinServerVersion(conn, 25, 2, 0) {
		t.Skip("Nullable(JSON) unsupported")
	}

	_, err := conn.ExecContext(ctx, "SET output_format_native_write_json_as_string = 1")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "SET output_format_json_quote_64bit_integers = 0")
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT arrayJoin(['{\"x\": 1}', '{\"x\": 2}', '{\"x\": 3}']::Array(Nullable(JSON)))")
	require.NoError(t, err)

	var rowStr string
	require.True(t, rows.Next())
	err = rows.Scan(&rowStr)
	require.NoError(t, err)
	require.Equal(t, "{\"x\":1}", rowStr)

	var rowBytes []byte
	require.True(t, rows.Next())
	err = rows.Scan(&rowBytes)
	require.NoError(t, err)
	require.Equal(t, []byte("{\"x\":2}"), rowBytes)

	var rowJSONRawMessage json.RawMessage
	require.True(t, rows.Next())
	err = rows.Scan(&rowJSONRawMessage)
	require.NoError(t, err)
	require.Equal(t, json.RawMessage("{\"x\":3}"), rowJSONRawMessage)

	require.NoError(t, rows.Close())
}
