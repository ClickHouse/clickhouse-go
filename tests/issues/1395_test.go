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

package issues

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1395(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse.Settings{}, false)
	conn, err := sql.Open("clickhouse", clickhouse_tests.OptionsToDSN(&opts))
	require.NoError(t, err)

	ctx := context.Background()

	singleConn, err := conn.Conn(ctx)
	if err != nil {
		t.Fatalf("Get single conn from pool: %v", err)
	}

	tx1 := func(c *sql.Conn) error {
		tx, err := c.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS test_table
ON CLUSTER my
(id UInt32, name String)
ENGINE = MergeTree()
ORDER BY id`)
		if err != nil {
			return fmt.Errorf("create table: %w", err)
		}

		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}

		return nil
	}

	err = tx1(singleConn)
	require.Error(t, err, "expected error due to cluster is not configured")

	tx2 := func(c *sql.Conn) error {
		tx, err := c.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, "INSERT INTO test_table (id, name) VALUES (?, ?)", 1, "test_name")
		if err != nil {
			return fmt.Errorf("failed to insert record: %w", err)
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("commit tx: %w", err)
		}

		return nil
	}
	require.NotPanics(
		t,
		func() {
			err := tx2(singleConn)
			require.ErrorIs(t, err, driver.ErrBadConn)
		},
		"must not panics",
	)
}
