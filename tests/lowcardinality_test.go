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
	"math/rand"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestLowCardinality(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_suspicious_low_cardinality_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 19, 11, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_lowcardinality (
			  Col1 LowCardinality(String)
			, Col2 LowCardinality(FixedString(2))
			, Col3 LowCardinality(DateTime)
			, Col4 LowCardinality(Int32)
			, Col5 Array(LowCardinality(String))
			, Col6 Array(Array(LowCardinality(String)))
			, Col7 LowCardinality(Nullable(String))
			, Col8 Array(Array(LowCardinality(Nullable(String))))
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_lowcardinality")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_lowcardinality")
	require.NoError(t, err)
	var (
		rnd       = rand.Int31()
		timestamp = time.Now()
	)
	for i := 0; i < 10; i++ {
		var (
			col1Data = timestamp.String()
			col2Data = "RU"
			col3Data = timestamp.Add(time.Duration(i) * time.Minute)
			col4Data = rnd + int32(i)
			col5Data = []string{"A", "B", "C"}
			col6Data = [][]string{
				[]string{"Q", "W", "E"},
				[]string{"R", "T", "Y"},
			}
			col7Data = &col2Data
			col8Data = [][]*string{
				[]*string{&col2Data, nil, &col2Data},
				[]*string{nil, &col2Data, nil},
			}
		)
		if i%2 == 0 {
			require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, col7Data, col8Data))
		} else {
			require.NoError(t, batch.Append(col1Data, col2Data, col3Data, col4Data, col5Data, col6Data, nil, col8Data))
		}
	}
	require.NoError(t, batch.Send())
	var count uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT COUNT() FROM test_lowcardinality").Scan(&count))
	assert.Equal(t, uint64(10), count)
	for i := 0; i < 10; i++ {
		var (
			col1 string
			col2 string
			col3 time.Time
			col4 int32
			col5 []string
			col6 [][]string
			col7 *string
			col8 [][]*string
		)
		require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_lowcardinality WHERE Col4 = $1", rnd+int32(i)).Scan(&col1, &col2, &col3, &col4, &col5, &col6, &col7, &col8))
		assert.Equal(t, timestamp.String(), col1)
		assert.Equal(t, "RU", col2)
		assert.Equal(t, timestamp.Add(time.Duration(i)*time.Minute).Truncate(time.Second).In(time.UTC), col3)
		assert.Equal(t, rnd+int32(i), col4)
		assert.Equal(t, []string{"A", "B", "C"}, col5)
		assert.Equal(t, [][]string{
			[]string{"Q", "W", "E"},
			[]string{"R", "T", "Y"},
		}, col6)
		switch {
		case i%2 == 0:
			assert.Equal(t, &col2, col7)
		default:
			assert.Nil(t, col7)
		}
		col2Data := "RU"
		assert.Equal(t, [][]*string{
			[]*string{&col2Data, nil, &col2Data},
			[]*string{nil, &col2Data, nil},
		}, col8)
	}
}

func TestColmunarLowCardinality(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_suspicious_low_cardinality_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_lowcardinality (
			  Col1 LowCardinality(String)
			, Col2 LowCardinality(FixedString(2))
			, Col3 LowCardinality(DateTime)
			, Col4 LowCardinality(Int32)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_lowcardinality")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_lowcardinality")
	require.NoError(t, err)
	var (
		rnd       = rand.Int31()
		timestamp = time.Now()
		col1Data  []string
		col2Data  []string
		col3Data  []time.Time
		col4Data  []int32
	)
	for i := 0; i < 10; i++ {
		col1Data = append(col1Data, timestamp.String())
		col2Data = append(col2Data, "RU")
		col3Data = append(col3Data, timestamp.Add(time.Duration(i)*time.Minute))
		col4Data = append(col4Data, rnd+int32(i))
	}
	require.NoError(t, batch.Column(0).Append(col1Data))
	require.NoError(t, batch.Column(1).Append(col2Data))
	require.NoError(t, batch.Column(2).Append(col3Data))
	require.NoError(t, batch.Column(3).Append(col4Data))
	require.NoError(t, batch.Send())
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT COUNT() FROM test_lowcardinality").Scan(&count); assert.NoError(t, err) {
		assert.Equal(t, uint64(10), count)
	}
	var (
		col1 string
		col2 string
		col3 time.Time
		col4 int32
	)
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_lowcardinality WHERE Col4 = $1", rnd+6).Scan(&col1, &col2, &col3, &col4))
	assert.Equal(t, timestamp.String(), col1)
	assert.Equal(t, "RU", col2)
	assert.Equal(t, timestamp.Add(time.Duration(6)*time.Minute).Truncate(time.Second).In(time.UTC), col3)
	assert.Equal(t, int32(rnd+6), col4)
}

func TestLowCardinalityFlush(t *testing.T) {
	conn, err := GetNativeConnection(clickhouse.Settings{
		"allow_suspicious_low_cardinality_types": 1,
	}, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 20, 1, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	const ddl = `
		CREATE TABLE test_lowcardinality_flush (
			  Col1 LowCardinality(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_lowcardinality_flush")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_lowcardinality_flush")
	require.NoError(t, err)
	vals := [100]string{}
	for i := 0; i < 100; i++ {
		vals[i] = RandAsciiString(10)
		require.NoError(t, batch.Append(vals[i]))
		require.NoError(t, batch.Flush())
	}
	require.NoError(t, batch.Send())
	rows, err := conn.Query(ctx, "SELECT * FROM test_lowcardinality_flush")
	require.NoError(t, err)
	i := 0
	for rows.Next() {
		var col1 string
		require.NoError(t, rows.Scan(&col1))
		require.Equal(t, vals[i], col1)
		i += 1
	}
	require.Equal(t, 100, i)
}
