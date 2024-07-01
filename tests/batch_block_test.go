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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

// TestBatchAppendRows tests experimental batch rows blocks append feature.
// This API is not stable and may be changed in the future.
func TestBatchAppendRows(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx := context.Background()

	// given we have two tables and a million rows in the source table
	var tables = []string{"source", "target"}
	for _, table := range tables {
		require.NoError(t, conn.Exec(context.Background(), "create table if not exists "+table+" (number1 Int, number2 LowCardinality(String), number3 Tuple(String, Int), number4 DateTime) engine = Memory()"))
		defer conn.Exec(context.Background(), "drop table if exists "+table)
	}

	require.NoError(t, conn.Exec(ctx, "INSERT INTO source SELECT number, toString(number), tuple('foo', number), now() FROM system.numbers LIMIT 1000000"))

	// when we create a batch with direct data block access 10 times
	sourceRows, err := conn.Query(ctx, "SELECT * FROM source")
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
	assert.Equal(t, 1000000, int(count))
}

// TestBatchColumns tests Batch.Columns() method functionality
func TestBatchColumns(t *testing.T) {
	ctx := context.Background()
	conn, err := GetNativeConnection(nil, nil, nil)
	require.NoError(t, err)
	// Prepare test table
	require.NoError(t, conn.Exec(ctx, `
		CREATE TABLE test_table (
		    Col1 Int,
			Col2 String
		) Engine MergeTree() ORDER BY tuple()
	`))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_table")
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_table")
	require.NoError(t, err)
	columns := batch.Columns()
	if assert.Len(t, columns, 2) {
		assert.IsType(t, new(column.Int32), columns[0])
		assert.Equal(t, "Col1", columns[0].Name())
		assert.IsType(t, new(column.String), columns[1])
		assert.Equal(t, "Col2", columns[1].Name())
	}
}
