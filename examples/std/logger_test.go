package std

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func StdLogger() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}

	// Create a structured logger with JSON output
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: logger,
	})
	defer db.Close()

	// All database operations will be logged with structured fields
	var count uint64
	if err := db.QueryRow("SELECT 1").Scan(&count); err != nil {
		return err
	}

	fmt.Println("Count: ", count)
	return nil
}

func StdTextLogger() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: logger,
	})
	defer db.Close()

	// Logs will be output in human-readable text format
	// Example: time=2024-01-21T10:00:00.000Z level=DEBUG msg="executing query" sql="SELECT 1" conn_id=1
	return nil
}

func StdLegacyDebug() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
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
	return nil
}

func StdEnrichedLogger() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}

	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// Add application-level context
	enrichedLogger := baseLogger.With(
		slog.String("service", "my-service"),
		slog.String("environment", "production"),
	)

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: enrichedLogger,
	})
	defer db.Close()

	// All logs will include service and environment fields
	return db.Ping()
}

func StdPoolLogging() error {
	env, err := GetStdTestEnvironment()
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
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
			return err
		}
	}

	// You'll see logs showing connection reuse across queries
	return nil
}
