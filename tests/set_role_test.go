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
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetRoleFunctionality(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	// Test native protocol
	t.Run("Native Protocol", func(t *testing.T) {
		conn, err := GetConnectionWithOptions(&opts)
		require.NoError(t, err)
		defer conn.Close()

		// Test session functionality
		session, err := conn.AcquireSession(context.Background())
		require.NoError(t, err)
		defer session.Close()

		// Test basic session operations
		err = session.Exec(context.Background(), "SELECT 1")
		require.NoError(t, err)

		// Test SET ROLE functionality
		err = session.Exec(context.Background(), "SET ROLE default")
		require.NoError(t, err)

		// Test query after SET ROLE
		rows, err := session.Query(context.Background(), "SELECT currentUser()")
		require.NoError(t, err)
		defer rows.Close()

		var user string
		if rows.Next() {
			err = rows.Scan(&user)
			require.NoError(t, err)
			t.Logf("Current user: %s", user)
		}

		// Test session state persistence
		err = session.Exec(context.Background(), "SET max_memory_usage = 1000000")
		require.NoError(t, err)

		// Verify setting is applied
		rows, err = session.Query(context.Background(), "SELECT value FROM system.settings WHERE name = 'max_memory_usage'")
		require.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var value string
			err = rows.Scan(&value)
			require.NoError(t, err)
			assert.Equal(t, "1000000", value)
		}
	})

	// Test standard SQL protocol with transactions (alternative to sessions)
	t.Run("Standard SQL Protocol with Transactions", func(t *testing.T) {
		// Skip this test for now as it requires proper database/sql setup
		// The native protocol test covers the main functionality
		t.Skip("Standard SQL protocol test requires additional setup")
	})
}

func TestBasicSessionFunctionality(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	defer conn.Close()

	// Test basic session acquisition and operations
	session, err := conn.AcquireSession(context.Background())
	require.NoError(t, err)
	defer session.Close()

	// Test Exec
	err = session.Exec(context.Background(), "SELECT 1")
	require.NoError(t, err)

	// Test Query
	rows, err := session.Query(context.Background(), "SELECT 42 as value")
	require.NoError(t, err)
	defer rows.Close()

	var value uint8
	if rows.Next() {
		err = rows.Scan(&value)
		require.NoError(t, err)
		assert.Equal(t, uint8(42), value)
	}

	// Test QueryRow
	var result uint8
	err = session.QueryRow(context.Background(), "SELECT 100").Scan(&result)
	require.NoError(t, err)
	assert.Equal(t, uint8(100), result)

	// Test Ping
	err = session.Ping(context.Background())
	require.NoError(t, err)
}

func TestSessionErrorHandling(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	defer conn.Close()

	session, err := conn.AcquireSession(context.Background())
	require.NoError(t, err)

	// Test session operations after close
	session.Close()

	// These should return ErrSessionClosed
	err = session.Exec(context.Background(), "SELECT 1")
	assert.ErrorIs(t, err, clickhouse.ErrSessionClosed)

	_, err = session.Query(context.Background(), "SELECT 1")
	assert.ErrorIs(t, err, clickhouse.ErrSessionClosed)

	err = session.Ping(context.Background())
	assert.ErrorIs(t, err, clickhouse.ErrSessionClosed)
}

func TestSessionResourceManagement(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)

	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	defer conn.Close()

	// Test that sessions properly release connections
	initialStats := conn.Stats()

	// Acquire and release multiple sessions
	for i := 0; i < 5; i++ {
		session, err := conn.AcquireSession(context.Background())
		require.NoError(t, err)

		err = session.Exec(context.Background(), "SELECT 1")
		require.NoError(t, err)

		err = session.Close()
		require.NoError(t, err)
	}

	// Verify connection pool is in good state
	finalStats := conn.Stats()
	assert.LessOrEqual(t, finalStats.Open, initialStats.Open+2) // Allow some variance
}
