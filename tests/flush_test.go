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
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBatchNoFlush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "cannot flush HTTP")

		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		insertWithFlush(t, protocol, conn, false)
	})
}

func TestBatchFlush(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		SkipOnHTTP(t, protocol, "cannot flush HTTP")

		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		require.NoError(t, err)
		insertWithFlush(t, protocol, conn, true)
	})
}

func insertWithFlush(t *testing.T, protocol clickhouse.Protocol, conn driver.Conn, flush bool) {
	ctx := context.Background()
	tableName := "batch_flush_example"
	if !flush {
		tableName = "batch_no_flush_example"
	}

	err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			  Col1 UInt64
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col5 Map(String, UInt64)
			, Col6 Array(String)
			, Col7 Tuple(String, UInt64, Array(Map(String, UInt64)))
			, Col8 DateTime
		) Engine = MergeTree() ORDER BY tuple()
	`, tableName))
	require.NoError(t, err)
	defer func() {
		conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s", tableName))
	}()

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", tableName))
	require.NoError(t, err)

	// 1 million rows should only take < 1s on most desktops
	for i := 0; i < 100_000; i++ {
		require.NoError(t, batch.Append(
			uint64(i),
			RandAsciiString(5),
			RandAsciiString(3),
			uuid.New(),
			map[string]uint64{"key": uint64(i)}, // Map(String, UInt64)
			[]string{RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1), RandAsciiString(1)}, // Array(String)
			[]any{ // Tuple(String, UInt64, Array(Map(String, UInt64)))
				RandAsciiString(10), uint64(i), []map[string]uint64{
					{"key": uint64(i)},
					{"key": uint64(i + 1)},
					{"key": uint64(i) + 2},
				},
			},
			time.Now().Add(time.Duration(i)*time.Second),
		))
		if i > 0 && i%10_000 == 0 {
			fmt.Printf("Rows = %d\t", batch.Rows())
			PrintMemUsage()

			if flush {
				require.NoError(t, batch.Flush())
			}
		}
	}
	require.NoError(t, batch.Send())

	// confirm we have the right count
	row := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s", tableName))
	require.NoError(t, row.Err())
	var col1 uint64
	require.NoError(t, row.Scan(&col1))
	require.Equal(t, uint64(100_000), col1)
}
