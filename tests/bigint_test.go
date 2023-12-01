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
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestSimpleBigInt(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_bigint (
			  Col1 Int128
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bigint")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bigint")
	require.NoError(t, err)
	col1Data, ok := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	require.True(t, ok)
	require.NoError(t, batch.Append(col1Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 big.Int
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bigint").Scan(&col1))
	assert.Equal(t, *col1Data, col1)

}

func TestBigInt(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_bigint (
			  Col1 Int128
			, Col2 UInt128
			, Col3 Array(Int128)
			, Col4 Int256
			, Col5 Array(Int256)
			, Col6 UInt256
			, Col7 Array(UInt256)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bigint")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bigint")
	require.NoError(t, err)
	col1Data, ok := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	require.True(t, ok)
	var (
		col2Data = big.NewInt(128)
		col3Data = []*big.Int{
			big.NewInt(-128),
			big.NewInt(128128),
			big.NewInt(128128128),
		}
		col4Data = big.NewInt(256)
		col5Data = []*big.Int{
			big.NewInt(256),
			big.NewInt(256256),
			big.NewInt(256256256256),
		}
		col6Data = big.NewInt(256)
		col7Data = []*big.Int{
			big.NewInt(256),
			big.NewInt(256256),
			big.NewInt(256256256256),
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 big.Int
		col2 big.Int
		col3 []*big.Int
		col4 big.Int
		col5 []*big.Int
		col6 big.Int
		col7 []*big.Int
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bigint").Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7))
	assert.Equal(t, *col1Data, col1)
	assert.Equal(t, *col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, *col4Data, col4)
	assert.Equal(t, col5Data, col5)
	assert.Equal(t, *col6Data, col6)
	assert.Equal(t, col7Data, col7)
}

func TestNullableBigInt(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_nullable_bigint (
			  Col1 Nullable(Int128)
			, Col2 Array(Nullable(Int128))
			, Col3 Nullable(Int256)
			, Col4 Array(Nullable(Int256))
			, Col5 Nullable(UInt256)
			, Col6 Array(Nullable(UInt256))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nullable_bigint")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nullable_bigint")
	require.NoError(t, err)
	var (
		col1Data = big.NewInt(128)
		col2Data = []*big.Int{
			big.NewInt(-128),
			big.NewInt(128128),
			big.NewInt(128128128),
		}
		col3Data = big.NewInt(256)
		col4Data = []*big.Int{
			big.NewInt(256),
			nil,
			big.NewInt(256256256256),
		}
		col5Data = big.NewInt(256)
		col6Data = []*big.Int{
			big.NewInt(256),
			big.NewInt(256256),
			big.NewInt(256256256256),
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 *big.Int
		col2 []*big.Int
		col3 *big.Int
		col4 []*big.Int
		col5 *big.Int
		col6 []*big.Int
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nullable_bigint").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	assert.Equal(t, *col1Data, *col1)
	assert.Equal(t, col2Data, col2)
	assert.Equal(t, *col3Data, *col3)
	assert.Equal(t, col4Data, col4)
	assert.Equal(t, *col5Data, *col5)
	assert.Equal(t, col6Data, col6)
}

func TestBigIntUIntOverflow(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_bigint_uint_overflow (
			  Col1 UInt128,
			  Col2 UInt128,
			  Col3 Array(UInt128),
			  Col4 UInt256,
			  Col5 UInt256,
			  Col6 Array(UInt256)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bigint_uint_overflow")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_bigint_uint_overflow")
	require.NoError(t, err)
	bigUint128Val := big.NewInt(0)
	bigUint128Val.SetString("170141183460469231731687303715884105729", 10)
	maxUint128Val := big.NewInt(0)
	maxUint128Val.SetString("340282366920938463463374607431768211455", 10)
	bigUint256Val := big.NewInt(0)
	bigUint256Val.SetString("57896044618658097711785492504343953926634992332820282019728792003956564819969", 10)
	maxUint256Val := big.NewInt(0)
	maxUint256Val.SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	var (
		col1Data = bigUint128Val
		col2Data = maxUint128Val

		col3Data = []*big.Int{
			big.NewInt(256),
			bigUint128Val,
			maxUint128Val,
		}

		col4Data = bigUint256Val
		col5Data = maxUint256Val

		col6Data = []*big.Int{
			big.NewInt(256),
			bigUint256Val,
			maxUint256Val,
		}
	)
	require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data))
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var (
		col1 big.Int
		col2 big.Int
		col3 []*big.Int
		col4 big.Int
		col5 big.Int
		col6 []*big.Int
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_bigint_uint_overflow").Scan(&col1, &col2, &col3, &col4, &col5, &col6))
	assert.Equal(t, *col1Data, col1)
	assert.Equal(t, *col2Data, col2)
	assert.Equal(t, col3Data, col3)
	assert.Equal(t, *col4Data, col4)
	assert.Equal(t, *col5Data, col5)
	assert.Equal(t, col6Data, col6)
}

func TestBigIntFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS big_int_flush")
	}()
	const ddl = `
		CREATE TABLE big_int_flush (
			  Col1 UInt128
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO big_int_flush")
	require.NoError(t, err)
	vals := [1000]*big.Int{}
	for i := 0; i < 1000; i++ {
		bigUint128Val := big.NewInt(0)
		bigUint128Val.SetString(RandIntString(20), 10)
		vals[i] = bigUint128Val
		batch.Append(vals[i])
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM big_int_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 big.Int
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, *vals[i], col1)
		i += 1
	}
}

type testBigIntSerializer struct {
	val *big.Int
}

func (c testBigIntSerializer) Value() (driver.Value, error) {
	return c.val, nil
}

func (c *testBigIntSerializer) Scan(src any) error {
	if t, ok := src.(*big.Int); ok {
		*c = testBigIntSerializer{val: t}
		return nil
	}
	return fmt.Errorf("cannot scan %T into testBigIntSerializer", src)
}

func TestBigIntValuer(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS big_int_flush")
	}()
	const ddl = `
		CREATE TABLE big_int_flush (
			  Col1 UInt128
		) Engine MergeTree() ORDER BY tuple()
		`
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO big_int_flush")
	require.NoError(t, err)
	vals := [1000]*big.Int{}
	for i := 0; i < 1000; i++ {
		bigUint128Val := big.NewInt(0)
		bigUint128Val.SetString(RandIntString(20), 10)
		vals[i] = bigUint128Val
		batch.Append(testBigIntSerializer{val: vals[i]})
		require.Equal(t, 1, batch.Rows())
		batch.Flush()
	}
	require.Equal(t, 0, batch.Rows())
	batch.Send()
	rows, err := conn.Query(ctx, "SELECT * FROM big_int_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 big.Int
		require.NoError(t, rows.Scan(&col1))
		assert.Equal(t, *vals[i], col1)
		i += 1
	}
}
