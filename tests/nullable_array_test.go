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
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestNullableArray(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	ctx := context.Background()
	const ddl = `
	CREATE TABLE test_nullable_array (
		  Col1  Array(Nullable(Bool))
		, Col2  Array(Nullable(UInt8))
		, Col3  Array(Nullable(Date))
		, Col4  Array(Nullable(Date32))
		, Col5  Array(Nullable(DateTime))
		, Col6  Array(Nullable(DateTime64))
		, Col7  Array(Nullable(Decimal(18,5)))
		, Col8  Array(Nullable(Enum8  ('click'   = 1,  'house' = 2)))
		, Col9  Array(Nullable(Enum16 ('click'   = 1,  'house' = 2)))
		, Col10 Array(Nullable(FixedString(5)))
		, Col11 Array(Nullable(IPv4))
		, Col12 Array(Nullable(IPv6))
		, Col13 Array(Nullable(String))
		, Col14 Array(Nullable(UUID))
	) Engine MergeTree() ORDER BY tuple()
	`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_nullable_array")
	}()
	require.NoError(t, err)
	if !CheckMinServerServerVersion(conn, 21, 12, 0) {
		t.Skip(fmt.Errorf("unsupported clickhouse version"))
		return
	}
	require.NoError(t, conn.Exec(ctx, ddl))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_nullable_array")
	require.NoError(t, err)
	var (
		strVal     = "ClickHouse"
		uint8Val   = uint8(42)
		boolTrue   = true
		boolFalse  = false
		decimalVal = decimal.New(25, 0)
		datetime   = time.Now().Truncate(time.Second)
		enum1Val   = "click"
		enum2Val   = "house"
		fixed1Val  = "Click"
		fixed2Val  = "House"
		uuidVal    = uuid.New()
		IPv4Val    = net.ParseIP("127.0.0.1")
		IPv6Val1   = net.ParseIP("2001:44c8:129:2632:33:0:252:2")
		IPv6Val2   = net.ParseIP("127.0.0.1")
		dateVal, _ = time.Parse("2006-01-02 15:04:05", "2022-01-12 00:00:00")
	)
	err = batch.Append(
		[]*bool{&boolTrue, nil, &boolFalse},
		[]*uint8{&uint8Val, nil, &uint8Val},
		[]*time.Time{&dateVal, nil, &dateVal},
		[]*time.Time{&dateVal, nil, &dateVal},
		[]*time.Time{&datetime, nil, &datetime},
		[]*time.Time{&datetime, nil, &datetime},
		[]*decimal.Decimal{&decimalVal, nil, &decimalVal},
		[]*string{&enum1Val, nil, &enum2Val},
		[]*string{&enum1Val, nil, &enum2Val},
		[]*string{&fixed1Val, nil, &fixed2Val},
		[]*net.IP{&IPv4Val, nil, &IPv4Val},
		[]*net.IP{&IPv6Val1, nil, &IPv6Val2},
		[]*string{&strVal, nil, &strVal},
		[]*uuid.UUID{&uuidVal, nil, &uuidVal},
	)
	require.NoError(t, err)
	require.Equal(t, 1, batch.Rows())
	require.NoError(t, batch.Send())
	var result struct {
		Col1  []*bool
		Col2  []*uint8
		Col3  []*time.Time
		Col4  []*time.Time
		Col5  []*time.Time
		Col6  []*time.Time
		Col7  []*decimal.Decimal
		Col8  []*string
		Col9  []*string
		Col10 []*string
		Col11 []*net.IP
		Col12 []*net.IP
		Col13 []*string
		Col14 []*uuid.UUID
	}
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_nullable_array").ScanStruct(&result))
	assert.Equal(t, []*bool{&boolTrue, nil, &boolFalse}, result.Col1)
	assert.Equal(t, []*uint8{&uint8Val, nil, &uint8Val}, result.Col2)
	assert.Equal(t, []*time.Time{&dateVal, nil, &dateVal}, result.Col3)
	assert.Equal(t, []*time.Time{&dateVal, nil, &dateVal}, result.Col4)
	utcDateTime := datetime.In(time.UTC)
	assert.Equal(t, []*time.Time{&utcDateTime, nil, &utcDateTime}, result.Col5)
	assert.Equal(t, []*time.Time{&utcDateTime, nil, &utcDateTime}, result.Col6)
	if assert.Nil(t, result.Col7[1]) {
		assert.True(t, decimalVal.Equal(*result.Col7[0]))
		assert.True(t, decimalVal.Equal(*result.Col7[2]))
	}
	assert.Equal(t, []*string{&enum1Val, nil, &enum2Val}, result.Col8)
	assert.Equal(t, []*string{&enum1Val, nil, &enum2Val}, result.Col9)
	assert.Equal(t, []*string{&fixed1Val, nil, &fixed2Val}, result.Col10)
	if assert.Nil(t, result.Col11[1]) {
		assert.Equal(t, IPv4Val.To4(), *result.Col11[0])
		assert.Equal(t, IPv4Val.To4(), *result.Col11[2])
	}
	if assert.Nil(t, result.Col12[1]) {
		assert.Equal(t, IPv6Val1, *result.Col12[0])
		assert.Equal(t, IPv6Val2, *result.Col12[2])
	}
	assert.Equal(t, []*string{&strVal, nil, &strVal}, result.Col13)
	assert.Equal(t, []*uuid.UUID{&uuidVal, nil, &uuidVal}, result.Col14)
}
