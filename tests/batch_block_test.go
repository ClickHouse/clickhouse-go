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
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

// TestBatchAppendRows tests experimental batch rows blocks append feature.
// This API is not stable and may be changed in the future.
func TestBatchAppendRows(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	blocksRead := atomic.Uint64{}
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{})
	opts.Debug = true
	opts.Debugf = func(format string, args ...interface{}) {
		if format == "[batch.appendRowsBlocks] blockNum = %d" {
			blocksRead.Store(uint64(args[0].(int))) // store the last block number read from rows
		}
	}

	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx := context.Background()

	// given we have two tables and a million rows in the source table
	var tables = []string{"source", "target"}
	for _, table := range tables {
		require.NoError(t, conn.Exec(context.Background(), "create table if not exists "+table+" (number1 Int, number2 String, number3 Tuple(String, Int), number4 DateTime) engine = MergeTree() order by tuple()"))
		defer conn.Exec(context.Background(), "drop table if exists "+table)
	}

	require.NoError(t, conn.Exec(ctx, "INSERT INTO source SELECT number, 'string', tuple('foo', number), now() FROM system.numbers LIMIT 1000000"))

	// when we create a batch with direct data block access 10 times

	selectCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 1000,
	}))

	sourceRows, err := conn.Query(selectCtx, "SELECT * FROM source")
	require.NoError(t, err)
	defer sourceRows.Close()

	b, err := conn.PrepareBatch(ctx, "INSERT INTO target")
	require.NoError(t, err)
	require.NoError(t, b.Append(sourceRows))
	require.NoError(t, b.Send())

	// then we should be able to see the data in the target table
	row := conn.QueryRow(ctx, "SELECT count() FROM source")
	require.NoError(t, row.Err())
	var count uint64
	require.NoError(t, row.Scan(&count))
	assert.Equal(t, uint64(1000000), count)
	assert.Equal(t, uint64(999), blocksRead.Load())
}
