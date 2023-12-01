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

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
)

func TestUUID(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_uuid (
				  Col1 UUID
				, Col2 UUID
				, Col3 Array(UUID)
				, Col4 Nullable(UUID)
				, Col5 Array(Nullable(UUID))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uuid")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uuid")
	require.NoError(t, err)
	var (
		col1Data = uuid.New()
		col2Data = uuid.New()
	)
	require.NoError(t, batch.Append(col1Data, col2Data, []uuid.UUID{col2Data, col1Data}, nil, []*uuid.UUID{
		&col1Data, nil, &col2Data,
	}))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 uuid.UUID
		col2 uuid.UUID
		col3 []uuid.UUID
		col4 *uuid.UUID
		col5 []*uuid.UUID
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uuid").Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Equal(t, col1Data, col1)
	assert.Equal(t, col2Data, col2)
	require.Nil(t, col4)
	assert.Equal(t, []uuid.UUID{col2Data, col1Data}, col3)
	assert.Equal(t, []*uuid.UUID{
		&col1Data, nil, &col2Data,
	}, col5)
}

func TestStringerUUID(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_uuid (
				  Col1 UUID
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uuid")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uuid")
	require.NoError(t, err)
	var (
		col1Data = uuid.New()
	)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 uuid.UUID
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uuid").Scan(&col1))
	assert.Equal(t, col1Data.String(), col1.String())
}

func TestNullableUUID(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_uuid (
				  Col1 Nullable(UUID)
				, Col2 Nullable(UUID)
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uuid")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uuid")
	require.NoError(t, err)
	var (
		col1Data = uuid.New()
		col2Data = uuid.New()
	)
	require.NoError(t, batch.Append(col1Data, col2Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 *uuid.UUID
		col2 *uuid.UUID
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uuid").Scan(&col1, &col2))
	assert.Equal(t, col1Data, *col1)
	assert.Equal(t, col2Data, *col2)
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE test_uuid"))
	batch, err = conn.PrepareBatch(ctx, "INSERT INTO test_uuid")
	require.NoError(t, err)
	{
		var col1Data = uuid.New()
		require.NoError(t, batch.Append(col1Data, nil))
		require.Equal(t, 1, batch.Rows())
		require.NoError(t, batch.Send())
		var (
			col1 *uuid.UUID
			col2 *uuid.UUID
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uuid").Scan(&col1, &col2))
		require.Nil(t, col2)
		assert.Equal(t, col1Data, *col1)
	}
}

func TestColumnarUUID(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
			CREATE TABLE test_uuid (
				  Col1 UUID
				, Col2 UUID
				, Col3 Nullable(UUID)
				, Col4 Array(UUID)
				, Col5 Array(Nullable(UUID))
			) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_uuid")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_uuid")
	require.NoError(t, err)
	var (
		col1Data []uuid.UUID
		col2Data []uuid.UUID
		col3Data []*uuid.UUID
		col4Data [][]uuid.UUID
		col5Data [][]*uuid.UUID
		v1, v2   = uuid.New(), uuid.New()
	)
	col1Data = append(col1Data, v1)
	col2Data = append(col2Data, v2)
	col3Data = append(col3Data, nil)
	col4Data = append(col4Data, []uuid.UUID{v1, v2})
	col5Data = append(col5Data, []*uuid.UUID{&v1, nil, &v2})
	for i := 0; i < 1000; i++ {
		require.NoError(t, batch.Column(0).Append(col1Data))
		require.NoError(t, batch.Column(1).Append(col2Data))
		require.NoError(t, batch.Column(2).Append(col3Data))
		require.NoError(t, batch.Column(3).Append(col4Data))
		require.NoError(t, batch.Column(4).Append(col5Data))
	}
	require.Equal(t, 1000, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 uuid.UUID
		col2 uuid.UUID
		col3 *uuid.UUID
		col4 []uuid.UUID
		col5 []*uuid.UUID
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_uuid LIMIT $1", 1).Scan(&col1, &col2, &col3, &col4, &col5))
	assert.Equal(t, v1, col1)
	assert.Equal(t, v2, col2)
	if assert.Nil(t, col3) {
		assert.Equal(t, []uuid.UUID{v1, v2}, col4)
		assert.Equal(t, []*uuid.UUID{&v1, nil, &v2}, col5)
	}
}

func BenchmarkUUID(b *testing.B) {
	ctx := context.Background()
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS benchmark_uuid")
	}()

	if err = conn.Exec(ctx, `CREATE TABLE benchmark_uuid (Col1 UInt64, Col2 UUID) ENGINE = Null`); err != nil {
		b.Fatal(err)
	}

	const rowsInBlock = 10_000_000
	value := uuid.New()
	for n := 0; n < b.N; n++ {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO benchmark_uuid VALUES")
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < rowsInBlock; i++ {
			if err := batch.Append(uint64(1), value); err != nil {
				b.Fatal(err)
			}
		}
		if err = batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}

func getTestUuids() (uuids []uuid.UUID, err error) {
	uuid1, err := uuid.Parse("603966d6-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}
	uuid2, err := uuid.Parse("60396956-ed93-11ec-8ea0-0242ac120002")
	if err != nil {
		return
	}

	uuids = []uuid.UUID{uuid1, uuid2}
	return
}

func TestUuid_ScanRow(t *testing.T) {
	uuids, err := getTestUuids()
	if err != nil {
		t.Fatal(err)
	}

	col := column.UUID{}
	_, err = col.Append(uuids)
	if err != nil {
		t.Fatal(err)
	}

	// scanning uuid.UUID
	for i := range uuids {
		var u uuid.UUID
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i] {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}

	// scanning strings
	for i := range uuids {
		var u string
		err := col.ScanRow(&u, i)
		if err != nil {
			require.Error(t, err, "unexpected ScanRow error")
		}
		if u != uuids[i].String() {
			require.Failf(t, "Invalid result of ScanRow", "ScanRow resulted in %q instead of %q", u, uuids[i])
		}
	}
}

func TestUUIDFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS uuid_flush")
	}()
	const ddl = `
		CREATE TABLE uuid_flush (
			  Col1 UUID
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO uuid_flush")
	require.NoError(t, err)
	vals := [1000]uuid.UUID{}
	for i := 0; i < 1000; i++ {
		vals[i] = uuid.New()
		batch.Append(vals[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM uuid_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 uuid.UUID
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}

type testUUIDValuer struct {
	val uuid.UUID
}

func (c testUUIDValuer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testUUIDValuer) Scan(src any) error {
	if t, ok := src.(string); ok {
		*c = testUUIDValuer{val: uuid.MustParse(t)}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testUUIDValuer", src)
}

func TestUUIDValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS uuid_valuer1")
	}()
	const ddl = `
		CREATE TABLE uuid_valuer1 (
			  Col1 UUID
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO uuid_valuer1")
	require.NoError(t, err)
	vals := [1000]uuid.UUID{}
	for i := 0; i < 1000; i++ {
		vals[i] = uuid.New()
		batch.Append(testUUIDValuer{val: vals[i]})
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM uuid_valuer1")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 uuid.UUID
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 1000, i)
}
