package clickhouse_api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
)

// LongRunningQueryWithProgressHeaders demonstrates how to handle long-running queries
// using HTTP progress headers to prevent load balancer idle timeouts
func LongRunningQueryWithProgressHeaders() error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{GetEnv("CLICKHOUSE_HOST", "localhost:8123")},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			// Enable progress in HTTP headers to keep connection alive
			// This prevents load balancers from closing idle connections
			"send_progress_in_http_headers":     1,
			"http_headers_progress_interval_ms": "110000", // 110 seconds - adjust based on your LB timeout
		},
		// Allow query to run for up to 10 minutes
		ReadTimeout: 10 * time.Minute,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := context.Background()

	// Create a test table
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS long_running_example`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE long_running_example (
			id UInt64,
			value String
		) ENGINE = MergeTree()
		ORDER BY id
	`); err != nil {
		return err
	}

	// Simulate a long-running insert
	// In production, this might be INSERT FROM SELECT on a large table
	fmt.Println("Starting long-running query...")
	err = conn.Exec(ctx, `
		INSERT INTO long_running_example
		SELECT number, toString(number)
		FROM numbers(10000000)
	`)
	if err != nil {
		return err
	}

	fmt.Println("Long-running query completed successfully")

	// Clean up
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS long_running_example`); err != nil {
		return err
	}

	return nil
}

// LongRunningQueryFireAndForget demonstrates the fire-and-forget approach
// where the client disconnects early and polls for completion
func LongRunningQueryFireAndForget() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx := context.Background()

	// Create test table
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS fire_forget_example`); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE fire_forget_example (
			id UInt64,
			value String
		) ENGINE = MergeTree()
		ORDER BY id
	`); err != nil {
		return err
	}

	// Generate unique query ID
	queryID := uuid.New().String()

	// Start the long-running mutation
	queryCtx := clickhouse.Context(context.Background(),
		clickhouse.WithQueryID(queryID),
	)

	// Execute query in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Exec(queryCtx, `
			INSERT INTO fire_forget_example
			SELECT number, toString(number)
			FROM numbers(10000000)
		`)
	}()

	// Wait for query to appear in system.query_log
	fmt.Println("Waiting for query to start...")
	if err := waitForQueryStart(conn, queryID, 30*time.Second); err != nil {
		return fmt.Errorf("query never started: %w", err)
	}

	fmt.Println("Query started on server, client can now disconnect")

	// Poll until completion
	fmt.Println("Polling for query completion...")
	if err := waitForQueryComplete(conn, queryID, 5*time.Minute); err != nil {
		return fmt.Errorf("query failed or timeout: %w", err)
	}

	fmt.Println("Query completed successfully")

	// Wait for background goroutine
	if err := <-errCh; err != nil {
		log.Printf("Background query result: %v", err)
	}

	// Clean up
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS fire_forget_example`); err != nil {
		return err
	}

	return nil
}

func waitForQueryStart(conn clickhouse.Conn, queryID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var exists uint8
		err := conn.QueryRow(context.Background(), `
			SELECT COUNT(*) > 0
			FROM system.query_log
			WHERE query_id = $1
		`, queryID).Scan(&exists)

		if err == nil && exists == 1 {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for query to start")
}

func waitForQueryComplete(conn clickhouse.Conn, queryID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var queryType string
		err := conn.QueryRow(context.Background(), `
			SELECT type
			FROM system.query_log
			WHERE query_id = $1 AND type != 'QueryStart'
			ORDER BY event_time DESC
			LIMIT 1
		`, queryID).Scan(&queryType)

		if err == nil {
			if queryType == "QueryFinish" {
				return nil
			}
			return fmt.Errorf("query failed with type: %s", queryType)
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timeout waiting for query completion")
}
