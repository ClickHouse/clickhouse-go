package clickhouse_api

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func Logger() error {
	env, err := GetNativeTestEnvironment()
	if err != nil {
		return err
	}

	// Create a structured logger with JSON output
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	// All connection operations will now be logged with structured fields
	// Output will include fields like: conn_id, remote_addr, protocol, etc.
	return nil
}

func LegacyDebug() error {
	env, err := GetNativeTestEnvironment()
	if err != nil {
		return err
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
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
	if err != nil {
		return err
	}
	defer conn.Close()

	// Legacy Debugf will be called for all log messages
	return nil
}

func TextLogger() error {
	env, err := GetNativeTestEnvironment()
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: logger,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	// Logs will be output in human-readable text format
	// Example: time=2024-01-21T10:00:00.000Z level=DEBUG msg="query" sql="SELECT 1" conn_id=1
	return nil
}

func EnrichedLogger() error {
	env, err := GetNativeTestEnvironment()
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
		slog.String("version", "1.0.0"),
	)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		Logger: enrichedLogger,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	// All logs will include service, environment, and version fields
	// in addition to the connection-specific fields
	return nil
}
