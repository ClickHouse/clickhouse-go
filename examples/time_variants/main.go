package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
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

	fmt.Println("=== ClickHouse Time and Time64 Variants Demo ===")

	// Step 1: Read and display all existing data
	fmt.Println("1. READING EXISTING DATA:")
	fmt.Println("==========================")

	rows, err := conn.Query(ctx, `
		SELECT 
			id,
			time_seconds,
			time64_3,
			time64_6,
			time64_9,
			description
		FROM time_variants_demo
		ORDER BY id
	`)
	if err != nil {
		log.Fatal("Error querying data:", err)
	}
	defer rows.Close()

	fmt.Printf("%-3s %-12s %-15s %-18s %-21s %-25s\n", "ID", "Time", "Time64(3)", "Time64(6)", "Time64(9)", "Description")
	fmt.Println(string(make([]byte, 100, 100)))

	for rows.Next() {
		var (
			id          uint32
			timeSeconds time.Time
			time64_3    time.Time
			time64_6    time.Time
			time64_9    time.Time
			description string
		)

		if err := rows.Scan(&id, &timeSeconds, &time64_3, &time64_6, &time64_9, &description); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		fmt.Printf("%-3d %-12s %-15s %-18s %-21s %-25s\n",
			id,
			timeSeconds.Format("15:04:05"),
			time64_3.Format("15:04:05.000"),
			time64_6.Format("15:04:05.000000"),
			time64_9.Format("15:04:05.000000000"),
			description)
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error iterating rows:", err)
	}

	// Step 2: Modify existing data
	fmt.Println("\n2. MODIFYING DATA:")
	fmt.Println("===================")

	// Update row with ID 1
	newTime := time.Date(2024, 1, 1, 14, 25, 30, 0, time.UTC)
	newTime64_3 := time.Date(2024, 1, 1, 14, 25, 30, 123000000, time.UTC)
	newTime64_6 := time.Date(2024, 1, 1, 14, 25, 30, 123456000, time.UTC)
	newTime64_9 := time.Date(2024, 1, 1, 14, 25, 30, 123456789, time.UTC)

	err = conn.Exec(ctx, `
		ALTER TABLE time_variants_demo 
		UPDATE 
			time_seconds = ?,
			time64_3 = ?,
			time64_6 = ?,
			time64_9 = ?,
			description = ?
		WHERE id = 1
	`, newTime, newTime64_3, newTime64_6, newTime64_9, "Updated morning time")

	if err != nil {
		log.Printf("Error updating data: %v", err)
	} else {
		fmt.Println("✓ Updated row with ID 1")
	}

	// Insert new row
	newRowTime := time.Date(2024, 1, 1, 18, 45, 15, 0, time.UTC)
	newRowTime64_3 := time.Date(2024, 1, 1, 18, 45, 15, 987000000, time.UTC)
	newRowTime64_6 := time.Date(2024, 1, 1, 18, 45, 15, 987654000, time.UTC)
	newRowTime64_9 := time.Date(2024, 1, 1, 18, 45, 15, 987654321, time.UTC)

	err = conn.Exec(ctx, `
		INSERT INTO time_variants_demo VALUES (?, ?, ?, ?, ?, ?)
	`, 6, newRowTime, newRowTime64_3, newRowTime64_6, newRowTime64_9, "New evening time")

	if err != nil {
		log.Printf("Error inserting new row: %v", err)
	} else {
		fmt.Println("✓ Inserted new row with ID 6")
	}

	// Step 3: Query and display modified data
	fmt.Println("\n3. QUERYING MODIFIED DATA:")
	fmt.Println("===========================")

	rows, err = conn.Query(ctx, `
		SELECT 
			id,
			time_seconds,
			time64_3,
			time64_6,
			time64_9,
			description
		FROM time_variants_demo
		ORDER BY id
	`)
	if err != nil {
		log.Fatal("Error querying modified data:", err)
	}
	defer rows.Close()

	fmt.Printf("%-3s %-12s %-15s %-18s %-21s %-25s\n", "ID", "Time", "Time64(3)", "Time64(6)", "Time64(9)", "Description")
	fmt.Println(string(make([]byte, 100, 100)))

	for rows.Next() {
		var (
			id          uint32
			timeSeconds time.Time
			time64_3    time.Time
			time64_6    time.Time
			time64_9    time.Time
			description string
		)

		if err := rows.Scan(&id, &timeSeconds, &time64_3, &time64_6, &time64_9, &description); err != nil {
			log.Printf("Error scanning modified row: %v", err)
			continue
		}

		fmt.Printf("%-3d %-12s %-15s %-18s %-21s %-25s\n",
			id,
			timeSeconds.Format("15:04:05"),
			time64_3.Format("15:04:05.000"),
			time64_6.Format("15:04:05.000000"),
			time64_9.Format("15:04:05.000000000"),
			description)
	}

	if err := rows.Err(); err != nil {
		log.Fatal("Error iterating modified rows:", err)
	}

	// Step 4: Demonstrate different query patterns
	fmt.Println("\n4. ADVANCED QUERY PATTERNS:")
	fmt.Println("============================")

	// Query specific time ranges
	fmt.Println("\n--- Times between 12:00 and 16:00 ---")
	rows, err = conn.Query(ctx, `
		SELECT id, time_seconds, description
		FROM time_variants_demo
		WHERE time_seconds BETWEEN '12:00:00' AND '16:00:00'
		ORDER BY time_seconds
	`)
	if err != nil {
		log.Printf("Error querying time range: %v", err)
	} else {
		for rows.Next() {
			var id uint32
			var timeSeconds time.Time
			var description string

			if err := rows.Scan(&id, &timeSeconds, &description); err != nil {
				log.Printf("Error scanning time range row: %v", err)
				continue
			}

			fmt.Printf("ID: %d, Time: %s, Description: %s\n",
				id, timeSeconds.Format("15:04:05"), description)
		}
		rows.Close()
	}

	// Query with precision comparison
	fmt.Println("\n--- Time64 with precision > 500ms ---")
	rows, err = conn.Query(ctx, `
		SELECT id, time64_3, time64_6, time64_9
		FROM time_variants_demo
		WHERE time64_3 > '00:00:00.500'
		ORDER BY time64_3
	`)
	if err != nil {
		log.Printf("Error querying precision: %v", err)
	} else {
		for rows.Next() {
			var id uint32
			var time64_3, time64_6, time64_9 time.Time

			if err := rows.Scan(&id, &time64_3, &time64_6, &time64_9); err != nil {
				log.Printf("Error scanning precision row: %v", err)
				continue
			}

			fmt.Printf("ID: %d, Time64(3): %s, Time64(6): %s, Time64(9): %s\n",
				id,
				time64_3.Format("15:04:05.000"),
				time64_6.Format("15:04:05.000000"),
				time64_9.Format("15:04:05.000000000"))
		}
		rows.Close()
	}

	fmt.Println("\n=== Demo Completed Successfully! ===")
}
