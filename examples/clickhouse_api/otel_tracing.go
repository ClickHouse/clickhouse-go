package clickhouse_api

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// OtelTracing demonstrates how to use OpenTelemetry tracing with clickhouse-go
// to measure client-side vs server-side time for database operations.
func OtelTracing() error {
	// Initialize OpenTelemetry with stdout exporter for demonstration
	shutdown, err := initTracer()
	if err != nil {
		return fmt.Errorf("failed to initialize tracer: %w", err)
	}
	defer shutdown()

	// Create a ClickHouse connection with OpenTelemetry tracing enabled
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: false,
		// Enable OpenTelemetry tracing
		OpenTelemetryOptions: []clickhouse.OtelOption{
			clickhouse.WithOtelEnabled(true),
			clickhouse.WithServerMetrics(true), // Capture server-side metrics
		},
	})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	fmt.Println("=== ClickHouse OpenTelemetry Tracing Demo ===\n")

	// Example 1: Simple Query
	fmt.Println("1. Running a simple query...")
	if err := exampleSimpleQuery(conn); err != nil {
		return fmt.Errorf("simple query failed: %w", err)
	}

	// Example 2: Query with Parameters
	fmt.Println("\n2. Running a query with parameters...")
	if err := exampleQueryWithParams(conn); err != nil {
		return fmt.Errorf("query with params failed: %w", err)
	}

	// Example 3: Batch Insert
	fmt.Println("\n3. Running a batch insert...")
	if err := exampleBatchInsert(conn); err != nil {
		return fmt.Errorf("batch insert failed: %w", err)
	}

	// Example 4: Query with Progress Tracking
	fmt.Println("\n4. Running a query with progress tracking...")
	if err := exampleQueryWithProgress(conn); err != nil {
		return fmt.Errorf("query with progress failed: %w", err)
	}

	fmt.Println("\n=== Demo Complete ===")
	fmt.Println("\nCheck the trace output above to see:")
	fmt.Println("- Total operation time (client-side)")
	fmt.Println("- Server elapsed time in attributes (db.clickhouse.server.elapsed_ns)")
	fmt.Println("- The difference shows network and serialization overhead")

	return nil
}

// exampleSimpleQuery demonstrates tracing a simple SELECT query
func exampleSimpleQuery(conn clickhouse.Conn) error {
	ctx := context.Background()

	var count uint64
	row := conn.QueryRow(ctx, "SELECT COUNT() FROM system.numbers LIMIT 1000000")
	if err := row.Scan(&count); err != nil {
		return err
	}

	fmt.Printf("   Count: %d\n", count)
	return nil
}

// exampleQueryWithParams demonstrates tracing a query with parameters
func exampleQueryWithParams(conn clickhouse.Conn) error {
	ctx := context.Background()

	var result string
	row := conn.QueryRow(ctx, "SELECT concat('Hello, ', ?)", "World")
	if err := row.Scan(&result); err != nil {
		return err
	}

	fmt.Printf("   Result: %s\n", result)
	return nil
}

// exampleBatchInsert demonstrates tracing a batch insert operation
func exampleBatchInsert(conn clickhouse.Conn) error {
	ctx := context.Background()

	// Create a temporary table
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS otel_demo"); err != nil {
		return err
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE otel_demo (
			id UInt64,
			name String,
			timestamp DateTime
		) ENGINE = Memory
	`); err != nil {
		return err
	}

	// Prepare batch
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO otel_demo")
	if err != nil {
		return err
	}

	// Append rows
	for i := 0; i < 100; i++ {
		if err := batch.Append(uint64(i), fmt.Sprintf("name_%d", i), time.Now()); err != nil {
			return err
		}
	}

	// Send batch
	if err := batch.Send(); err != nil {
		return err
	}

	fmt.Printf("   Inserted 100 rows\n")

	// Clean up
	if err := conn.Exec(ctx, "DROP TABLE otel_demo"); err != nil {
		return err
	}

	return nil
}

// exampleQueryWithProgress demonstrates tracing with server-side progress metrics
func exampleQueryWithProgress(conn clickhouse.Conn) error {
	ctx := context.Background()

	// Add progress callback to capture server-side metrics
	// The tracing system will automatically attach these metrics to the span
	ctx = clickhouse.Context(ctx,
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
			fmt.Printf("   Progress: rows=%d, bytes=%d, elapsed=%v\n",
				p.Rows, p.Bytes, p.Elapsed)
		}),
		clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
			fmt.Printf("   Profile: rows=%d, blocks=%d, bytes=%d\n",
				p.Rows, p.Blocks, p.Bytes)
		}),
	)

	rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10000")
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var num uint64
		if err := rows.Scan(&num); err != nil {
			return err
		}
		count++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	fmt.Printf("   Read %d rows\n", count)
	return nil
}

// initTracer initializes the OpenTelemetry tracer with stdout exporter
func initTracer() (func(), error) {
	// Create stdout exporter for demonstration
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	// Create tracer provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("clickhouse-go-demo"),
			attribute.String("environment", "development"),
		)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Return shutdown function
	return func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}
