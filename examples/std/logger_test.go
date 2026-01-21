package std_test

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func Example_stdLogger() {
	// Create a structured logger with JSON output
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Logger: logger,
	})
	defer db.Close()

	// All database operations will be logged with structured fields
	var count uint64
	if err := db.QueryRow("SELECT count() FROM system.numbers LIMIT 1").Scan(&count); err != nil {
		fmt.Printf("Query failed: %v\n", err)
		return
	}

	fmt.Println("Count: ", count)
}

func Example_stdTextLogger() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:   []string{"localhost:9000"},
		Logger: logger,
	})
	defer db.Close()

	// Logs will be output in human-readable text format
	// Example: time=2024-01-21T10:00:00.000Z level=DEBUG msg="executing query" sql="SELECT 1" conn_id=1
}

func Example_stdLegacyDebug() {
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf("[LEGACY] "+format+"\n", v...)
		},
	})
	defer db.Close()

	// Legacy Debugf will be called for all log messages
	var result int
	db.QueryRow("SELECT 1").Scan(&result)
}

func Example_stdEnrichedLogger() {
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Add application-level context
	enrichedLogger := baseLogger.With(
		slog.String("service", "my-service"),
		slog.String("environment", "production"),
	)

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:   []string{"localhost:9000"},
		Logger: enrichedLogger,
	})
	defer db.Close()

	// All logs will include service and environment fields
	db.Ping()
}

func Example_stdPoolLogging() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:         []string{"localhost:9000"},
		Logger:       logger,
		MaxOpenConns: 5,
		MaxIdleConns: 2,
	})
	defer db.Close()

	// Pool operations like connection acquisition/release will be logged
	// with conn_id to track individual connections
	var result int
	for i := 0; i < 10; i++ {
		if err := db.QueryRow("SELECT ?", i).Scan(&result); err != nil {
			fmt.Printf("Query failed: %v\n", err)
			return
		}
	}

	// You'll see logs showing connection reuse across queries
}
