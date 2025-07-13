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

package clickhouse_api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// SetRoleExample demonstrates how to use SET ROLE functionality
// with the new session management feature
func SetRoleExample() error {
	ctx := context.Background()

	// Open connection
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Debug:            false,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	// Acquire a session for stateful operations
	session, err := conn.AcquireSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire session: %w", err)
	}
	defer session.Close()

	// Set a role for this session
	if err := session.Exec(ctx, "SET ROLE some_role"); err != nil {
		return fmt.Errorf("failed to set role: %w", err)
	}

	// Execute queries that will use the role
	var result string
	if err := session.QueryRow(ctx, "SELECT currentUser()").Scan(&result); err != nil {
		return fmt.Errorf("failed to query current user: %w", err)
	}
	fmt.Printf("Current user: %s\n", result)

	// Execute another query that will also use the role
	var count int
	if err := session.QueryRow(ctx, "SELECT count() FROM system.tables").Scan(&count); err != nil {
		return fmt.Errorf("failed to query table count: %w", err)
	}
	fmt.Printf("Table count: %d\n", count)

	// The session maintains the role across multiple operations
	if err := session.Exec(ctx, "SELECT 1"); err != nil {
		return fmt.Errorf("failed to execute simple query: %w", err)
	}

	fmt.Println("Session operations completed successfully")
	return nil
}

// SessionBatchExample demonstrates using sessions with batch operations
func SessionBatchExample() error {
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Debug:            false,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	// Create a test table
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS session_test (
			id UInt32,
			name String,
			created_at DateTime
		) ENGINE = Memory
	`); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Acquire a session
	session, err := conn.AcquireSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire session: %w", err)
	}
	defer session.Close()

	// Set role in session
	if err := session.Exec(ctx, "SET ROLE some_role"); err != nil {
		log.Printf("Warning: failed to set role (this is expected if role doesn't exist): %v", err)
	}

	// Prepare a batch using the session
	batch, err := session.PrepareBatch(ctx, "INSERT INTO session_test (id, name, created_at)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	// Add data to the batch
	for i := 0; i < 10; i++ {
		if err := batch.Append(
			uint32(i),
			fmt.Sprintf("item_%d", i),
			time.Now(),
		); err != nil {
			return fmt.Errorf("failed to append to batch: %w", err)
		}
	}

	// Send the batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	// Query the data using the same session (maintains role)
	rows, err := session.Query(ctx, "SELECT id, name, created_at FROM session_test ORDER BY id")
	if err != nil {
		return fmt.Errorf("failed to query data: %w", err)
	}
	defer rows.Close()

	fmt.Println("Inserted data:")
	for rows.Next() {
		var (
			id        uint32
			name      string
			createdAt time.Time
		)
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		fmt.Printf("  ID: %d, Name: %s, Created: %s\n", id, name, createdAt.Format(time.RFC3339))
	}

	// Clean up
	if err := conn.Exec(ctx, "DROP TABLE session_test"); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	fmt.Println("Session batch example completed successfully")
	return nil
}

func main() {
	if err := SetRoleExample(); err != nil {
		log.Fatalf("SetRoleExample failed: %v", err)
	}

	if err := SessionBatchExample(); err != nil {
		log.Fatalf("SessionBatchExample failed: %v", err)
	}
}
