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

func TestStdPreparedSelect(t *testing.T) {
	db, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	stmt, err := db.PrepareContext(ctx, "SELECT ? + ?")
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt.Close() })

	rows, err := stmt.QueryContext(ctx, 10, 5)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows.Close() })

	require.True(t, rows.Next())
	var sum int64
	require.NoError(t, rows.Scan(&sum))
	require.EqualValues(t, 15, sum)
	require.NoError(t, rows.Err())
}

// Test for prepared selects using both positional and named params.
func TestStdPreparedFunds(t *testing.T) {
	db, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	require.NoError(t, db.PingContext(ctx))

	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS std_prepared_funds")
	_, err = db.ExecContext(ctx, `
		CREATE TABLE std_prepared_funds (
			symbol String,
			name   String
		) Engine = Memory`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS std_prepared_funds") })

	_, err = db.ExecContext(ctx, `INSERT INTO std_prepared_funds (symbol, name) VALUES ('abc', 'ABC Fund')`)
	require.NoError(t, err)

	// q1: positional placeholder
	stmt1, err := db.PrepareContext(ctx, `SELECT name FROM std_prepared_funds WHERE symbol=? LIMIT 1`)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt1.Close() })
	rows1, err := stmt1.QueryContext(ctx, "abc")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows1.Close() })
	require.True(t, rows1.Next())
	var name1 string
	require.NoError(t, rows1.Scan(&name1))
	require.Equal(t, "ABC Fund", name1)
	require.NoError(t, rows1.Err())

	// q2: named query parameter
	stmt2, err := db.PrepareContext(ctx, `SELECT name FROM std_prepared_funds WHERE symbol={symbol: String} LIMIT 1`)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stmt2.Close() })
	rows2, err := stmt2.QueryContext(ctx, clickhouse.Named("symbol", "abc"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows2.Close() })
	require.True(t, rows2.Next())
	var name2 string
	require.NoError(t, rows2.Scan(&name2))
	require.Equal(t, "ABC Fund", name2)
	require.NoError(t, rows2.Err())
}
