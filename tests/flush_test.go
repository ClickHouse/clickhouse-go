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
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestNoFlushWithCompression(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	require.NoError(t, err)
	insertWithFlush(t, conn, false)
}

func TestFlushWithCompression(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	require.NoError(t, err)
	insertWithFlush(t, conn, true)
}

func TestFlush(t *testing.T) {
	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})
	require.NoError(t, err)
	insertWithFlush(t, conn, true)
}

func insertWithFlush(t *testing.T, conn driver.Conn, flush bool) {
	defer func() {
		conn.Exec(context.Background(), "DROP TABLE flush_example")
	}()
	conn.Exec(context.Background(), "DROP TABLE IF EXISTS flush_example")
	ctx := context.Background()
	err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS flush_example (
			  Col1 UInt64
			, Col2 String
			, Col3 FixedString(3)
			, Col4 UUID
			, Col5 Map(String, UInt64)
			, Col6 Array(String)
			, Col7 Tuple(String, UInt64, Array(Map(String, UInt64)))
			, Col8 DateTime
		) Engine = MergeTree() ORDER BY tuple()
	`)
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO flush_example")
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
		if i > 0 && i%10000 == 0 {
			if flush {
				require.NoError(t, batch.Flush())
			}
			PrintMemUsage()
		}
	}
	require.NoError(t, batch.Send())
	// confirm we have the right count
	var col1 uint64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count() FROM flush_example").Scan(&col1))
	require.Equal(t, uint64(100_000), col1)
}
