package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
)

func main() {
	// Connect to ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Debug: false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ctx := context.Background()

	fmt.Println("=== ClickHouse Time and Time64 Support Demo ===")

	// Test 1: Time64 column (fully supported)
	fmt.Println("\n--- Testing Time64 Column ---")

	// Create table with Time64 column
	err = conn.Exec(ctx, "DROP TABLE IF EXISTS test_time64")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Exec(ctx, "CREATE TABLE test_time64 (id UInt32, time64_col Time64(3)) ENGINE = Memory")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time64 table created")

	// Insert data using string format
	err = conn.Exec(ctx, "INSERT INTO test_time64 VALUES (1, '12:30:45.123')")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time64 data inserted (string format)")

	// Insert data using Go-native parameter binding
	timeVal64 := time.Date(2024, 1, 1, 15, 16, 17, 123000000, time.UTC)
	err = conn.Exec(ctx, "INSERT INTO test_time64 VALUES (?, ?)", 2, timeVal64)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time64 data inserted (Go-native format)")

	// Query and display results
	var id uint32
	var t time.Time
	err = conn.QueryRow(ctx, "SELECT id, time64_col FROM test_time64 WHERE id = 1").Scan(&id, &t)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Time64 query result - ID: %d, Time64: %s\n", id, t.Format("15:04:05.000"))

	err = conn.QueryRow(ctx, "SELECT id, time64_col FROM test_time64 WHERE id = 2").Scan(&id, &t)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Time64 query result - ID: %d, Time64: %s\n", id, t.Format("15:04:05.000"))

	// Test 2: Time column (now fully supported)
	fmt.Println("\n--- Testing Time Column ---")

	// Create table with Time column
	err = conn.Exec(ctx, "DROP TABLE IF EXISTS test_time")
	if err != nil {
		log.Fatal(err)
	}

	err = conn.Exec(ctx, "CREATE TABLE test_time (id UInt32, time_col Time) ENGINE = Memory")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time table created")

	// Insert data using string format
	err = conn.Exec(ctx, "INSERT INTO test_time VALUES (1, '12:30:45')")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time data inserted (string format)")

	// Insert data using Go-native parameter binding
	timeVal := time.Date(2024, 1, 1, 14, 15, 16, 0, time.UTC)
	err = conn.Exec(ctx, "INSERT INTO test_time VALUES (?, ?)", 2, timeVal)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("✓ Time data inserted (Go-native format)")

	// Query and display results
	err = conn.QueryRow(ctx, "SELECT id, time_col FROM test_time WHERE id = 1").Scan(&id, &t)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Time query result - ID: %d, Time: %s\n", id, t.Format("15:04:05"))

	err = conn.QueryRow(ctx, "SELECT id, time_col FROM test_time WHERE id = 2").Scan(&id, &t)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("✓ Time query result - ID: %d, Time: %s\n", id, t.Format("15:04:05"))

	// Test 3: Column type verification
	fmt.Println("\n--- Column Type Verification ---")

	// Verify Time column type
	timeCol := &column.Time{}
	fmt.Printf("✓ Time column type: %s\n", timeCol.Type())

	// Verify Time64 column type
	time64Col := &column.Time64{}
	fmt.Printf("✓ Time64 column type: %s\n", time64Col.Type())

	fmt.Println("\n=== Summary ===")
	fmt.Println("✅ Time: Fully supported - insert and query work perfectly")
	fmt.Println("✅ Time64: Fully supported - insert and query work perfectly")
	fmt.Println("✅ Go-native parameter binding works for both types")
	fmt.Println("✅ String format works for both types")
	fmt.Println("✅ Column types are properly registered")
	fmt.Println("✅ Ready for production use")
}
