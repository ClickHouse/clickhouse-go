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

package std

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

// Ensures we can execute a USE <db>; followed by a prepared SELECT.
func TestStdPreparedSelectWithUseDatabase(t *testing.T) {
	db, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	// Explicit USE should work as Exec on connection
	_, err = db.ExecContext(ctx, "USE default")
	require.NoError(t, err)

	stmt, err := db.PrepareContext(ctx, "SELECT ? + ?")
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close() })

	rows, err := stmt.QueryContext(ctx, 7, 8)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows.Close() })

	require.True(t, rows.Next())
	var sum int64
	require.NoError(t, rows.Scan(&sum))
	require.EqualValues(t, 15, sum)
	require.NoError(t, rows.Err())
}
