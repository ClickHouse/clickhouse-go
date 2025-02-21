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
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

var testDate, _ = time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", "2022-05-25 17:20:57 +0100 WEST")

func TestTuple(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	loc, err := time.LoadLocation("Europe/Lisbon")
	require.NoError(t, err)
	localTime := testDate.In(loc)

	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_tuple (
			  Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime('Europe/Lisbon'))
			, Col3 Tuple(name1 DateTime('Europe/Lisbon'), name2 FixedString(2), name3 Map(String, String))
			, Col4 Array(Array( Tuple(String, Int64) ))
			, Col5 Tuple(LowCardinality(String),           Array(LowCardinality(String)))
			, Col6 Tuple(LowCardinality(Nullable(String)), Array(LowCardinality(Nullable(String))))
			, Col7 Tuple(String, Int64)
			, Col8 Tuple(Nullable(String),Nullable(String))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = []any{"A", int64(42)}
		col2Data = []any{"B", int8(1), localTime.Truncate(time.Second)}
		col3Data = map[string]any{
			"name1": localTime.Truncate(time.Second),
			"name2": "CH",
			"name3": map[string]string{
				"key": "value",
			},
		}
		col4Data = [][][]any{
			[][]any{
				[]any{"Hi", int64(42)},
			},
		}
		col5Data = []any{
			"LCString",
			[]string{"A", "B", "C"},
		}
		str      = "LCString"
		col6Data = []any{
			&str,
			[]*string{&str, nil, &str},
		}
		col8Val  = "G"
		col7Data = &[]any{"C", int64(42)}
		col8Data = []any{&col8Val, (*string)(nil)}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data, col8Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []any
		col2 []any
		// col3 is a named tuple - we can use map
		col3 map[string]any
		col4 [][][]any
		col5 []any
		col6 []any
		col7 []any
		col8 []any
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8))
	assert.NoError(t, err)
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, col6Data, col6)
	assert.Equal(t, col7Data, &col7)
	assert.Equal(t, col8Data, col8)
}

func TestNamedTupleWithSlice(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(name String, `1` Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	// this will fail, slices can only be strongly typed if all slice elements are the same type - see TestNamedTupleWithTypedSlice
	require.Error(t, batch.Append([]string{"A", "2"}))
	batch, _ = conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	var (
		col1Data = []any{"A", int64(42)}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []any
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

func TestNamedTupleWithTypedSlice(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(name String, city String), Col2 Int32) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = []string{"Dale", "Lisbon"}
		name     = "Geoff"
		city     = "Chicago"
		col2Data = []*string{&name, &city}
	)
	require.NoError(t, batch.Append(col1Data, int32(0)))
	require.NoError(t, batch.Append(col2Data, int32(1)))
	require.Equal(t, 2, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 []string
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT Col1 FROM test_tuple ORDER BY Col2 ASC").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// named tuples work with maps
func TestNamedTupleWithMap(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(name String, id Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	// this will fail - see TestNamedTupleWithTypedMap as tuple needs to be same type
	require.Error(t, batch.Append(map[string]string{"name": "A", "id": "1"}))
	batch, _ = conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	col1Data := map[string]any{"name": "A", "id": int64(1)}
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 map[string]any
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// named tuples work with typed maps
func TestNamedTupleWithTypedMap(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(id Int64, code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	// typed maps can be used provided the Tuple is consistent
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = map[string]int64{"code": int64(1), "id": int64(2)}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 map[string]int64
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// named tuples work with typed structs
func TestNamedTupleWithStruct(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(Id Int64, Code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			Code int64
			Id   int64
		}{
			Code: 1,
			Id:   2,
		}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 struct {
			Code int64
			Id   int64
		}
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// named tuples work with typed structs tags
func TestNamedTupleWithStructTags(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(id Int64, code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			Code int64 `ch:"code"`
			Id   int64 `ch:"id"`
		}{
			Code: 1,
			Id:   2,
		}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 struct {
			Code int64 `ch:"code"`
			Id   int64 `ch:"id"`
		}
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

// named tuples will not work with unexported fields
func TestNamedTupleWithUnexportedStructField(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(id Int64, code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			foo int64 // unexported field shouldn't be counted.
			Bar int64
		}{}
	)
	err = batch.Append(col1Data)
	require.Error(t, err)
	require.Equal(t, "clickhouse [AppendRow]: (Col1 Tuple(id Int64, code Int64)) invalid size. expected 2 got 1", err.Error())
}

// named tuples will not work with too many fields
func TestNamedTupleWithTooManyFields(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(id Int64, code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			Foo int64
			Bar int64
			Baz int64
		}{}
	)
	err = batch.Append(col1Data)
	require.Error(t, err)
	require.Equal(t, "clickhouse [AppendRow]: (Col1 Tuple(id Int64, code Int64)) invalid size. expected 2 got 3", err.Error())
}

// named tuples will not work with invalid tags
func TestNamedTupleWithDuplicateTags(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(id Int64, code Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			Id   int64 `ch:"id"`
			Code int64 `ch:"id"` // duplicate tag, should be counted only once.
		}{}
	)
	err = batch.Append(col1Data)
	require.Error(t, err)
	require.Equal(t, "clickhouse [AppendRow]: (Col1 Tuple(id Int64, code Int64)) invalid size. expected 2 got 1", err.Error())
}

// test column names which need escaping
func TestNamedTupleWithEscapedColumns(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(`56` String, `a22\\`` Int64)) Engine MergeTree() ORDER BY tuple()"
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = map[string]any{"56": "A", "a22`": int64(1)}
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var col1 map[string]any
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1))
	assert.Equal(t, col1Data, col1)
}

func TestNamedTupleIncomplete(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(name String, id Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	require.Error(t, batch.Append(map[string]any{"name": "A"}))
	require.Error(t, batch.Append([]any{"Dale"}))
}

// unnamed tuples will not work with maps - keys cannot be attributed to fields
func TestUnNamedTupleWithMap(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(String, Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = map[string]any{"name": "A", "id": int64(1)}
	)
	// this will fail - maps can't be used for unnamed tuples
	err = batch.Append(col1Data)
	require.Error(t, err)
	require.Equal(t, "clickhouse [AppendRow]: (Col1 Tuple(String, Int64)) converting from map[string]interface {} is not supported for unnamed tuples - use a slice", err.Error())
	// insert some data properly to test scan - can't reuse batch
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	require.NoError(t, batch.Append([]any{"A", int64(42)}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var col1 map[string]any
	err = conn.QueryRow(ctx, "SELECT * FROM test_tuple").Scan(&col1)
	require.Error(t, err)
	require.Equal(t, "clickhouse [ScanRow]: (Col1) converting Tuple(String, Int64) to map[string]interface {} is unsupported. cannot use maps for unnamed tuples, use slice", err.Error())
}

// unnamed tuples will not work with structs - keys cannot be attributed to fields
func TestUnNamedTupleWithStruct(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	// https://github.com/ClickHouse/ClickHouse/pull/36544
	if !CheckMinServerServerVersion(conn, 22, 5, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = "CREATE TABLE test_tuple (Col1 Tuple(String, Int64)) Engine MergeTree() ORDER BY tuple()"

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		col1Data = struct {
			Name string
			Id   int64
		}{
			Name: "a",
			Id:   1,
		}
	)
	// this will fail - struct can't be used for unnamed tuples
	err = batch.Append(col1Data)
	require.Error(t, err)
	require.Equal(t, "clickhouse [AppendRow]: (Col1 Tuple(String, Int64)) converting from struct { Name string; Id int64 } is not supported for unnamed tuples - use a slice", err.Error())
}

func TestColumnarTuple(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_tuple (
			  ID   UInt64
			, Col1 Tuple(String, Int64)
			, Col2 Tuple(String, Int8, DateTime)
			, Col3 Tuple(DateTime, FixedString(2), Map(String, String))
			, Col4 Tuple(String, Int64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple")
	require.NoError(t, err)
	var (
		id        []uint64
		col1Data  = [][]any{}
		col2Data  = [][]any{}
		col3Data  = [][]any{}
		col4Data  = []*[]any{}
		timestamp = time.Now().Truncate(time.Second)
	)
	for i := 0; i < 1000; i++ {
		id = append(id, uint64(i))
		col1Data = append(col1Data, []any{
			fmt.Sprintf("A_%d", i), int64(i),
		})
		col2Data = append(col2Data, []any{
			fmt.Sprintf("B_%d", i), int8(1), timestamp,
		})
		col3Data = append(col3Data, []any{
			timestamp, "CH", map[string]string{
				"key": "value",
			},
		})
		col4Data = append(col4Data, &[]any{
			fmt.Sprintf("C_%d", i), int64(i),
		})
	}
	require.NoError(t, batch.Column(0).Append(id))
	require.NoError(t, batch.Column(1).Append(col1Data))
	require.NoError(t, batch.Column(2).Append(col2Data))
	require.NoError(t, batch.Column(3).Append(col3Data))
	require.NoError(t, batch.Column(4).Append(col4Data))
	require.Equal(t, 1000, batch.Rows())
	require.NoError(t, batch.Send())
	{
		var (
			id       uint64
			col1     []any
			col2     []any
			col3     []any
			col4     []any
			col1Data = []any{
				"A_542", int64(542),
			}
			col2Data = []any{
				"B_542", int8(1), timestamp.In(time.UTC),
			}
			col3Data = []any{
				timestamp.In(time.UTC), "CH", map[string]string{
					"key": "value",
				},
			}
			col4Data = &[]any{
				"C_542", int64(542),
			}
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_tuple WHERE ID = $1", 542).Scan(&id, &col1, &col2, &col3, &col4))
		assert.Equal(t, col1Data, col1)
		assert.Equal(t, col2Data, col2)
		assert.Equal(t, col3Data, col3)
		assert.Equal(t, col4Data, &col4)
	}
}

func TestTupleFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_tuple_flush (
			Col1 Tuple(name String, id Int64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple_flush")
	require.NoError(t, err)
	vals := [1000]map[string]any{}
	for i := 0; i < 1000; i++ {
		vals[i] = map[string]any{
			"id":   int64(i),
			"name": RandAsciiString(10),
		}
		require.NoError(t, batch.Append(vals[i]))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_tuple_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 map[string]any
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

type testTupleSerializer struct {
	val map[string]any
}

func (c testTupleSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testTupleSerializer) Scan(src any) error {
	if t, ok := src.(map[string]any); ok {
		*c = testTupleSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testTupleSerializer", src)
}

func TestTupleValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, nil)
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 9, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_tuple_valuer (
			Col1 Tuple(name String, id Int64)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_tuple_valuer")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_tuple_valuer")
	require.NoError(t, err)
	vals := [1000]map[string]any{}
	for i := 0; i < 1000; i++ {
		vals[i] = map[string]any{
			"id":   int64(i),
			"name": RandAsciiString(10),
		}
		require.NoError(t, batch.Append(testTupleSerializer{val: vals[i]}))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Flush())
	}
	require.Equal(t, 0, batch.Rows())
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_tuple_valuer")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 map[string]any
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
