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
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBindArrayInsert(t *testing.T) {

	conn, err := GetNativeConnection(nil, nil, &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	})

	ctx := context.Background()
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE test_bind_array_insert (
			  Col1 String
			, Col2 Array(String)
		) Engine MergeTree() ORDER BY tuple()
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_bind_array_insert")
	}()
	require.NoError(t, conn.Exec(ctx, ddl))
	arrayData := []string{"a", "b", "c"}
	err = conn.Exec(ctx, "INSERT INTO test_bind_array_insert (Col1, Col2) VALUES (?, ?)",
		"abc123", arrayData)
	require.NoError(t, err)
}
