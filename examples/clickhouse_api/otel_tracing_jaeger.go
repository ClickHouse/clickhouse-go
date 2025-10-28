package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("=== ClickHouse OpenTelemetry + Jaeger Demo ===\n")
	fmt.Println("Prerequisites:")
	fmt.Println("1. ClickHouse running on localhost:9000")
	fmt.Println("2. Jaeger running on localhost:4317 (OTLP)")
	fmt.Println()
	fmt.Println("Quick start:")
	fmt.Println("  docker run -d --name clickhouse --network host clickhouse/clickhouse-server")
	fmt.Println("  docker run -d --name jaeger --network host -e COLLECTOR_OTLP_ENABLED=true jaegertracing/all-in-one")
	fmt.Println()

	// Initialize OpenTelemetry with Jaeger
	shutdown, err := initTracer()
	if err != nil {
		log.Fatalf("Failed to initialize tracer: %v", err)
	}
	defer shutdown()

	// Create ClickHouse connection with tracing enabled
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		OpenTelemetryOptions: []clickhouse.OtelOption{
			clickhouse.WithOtelEnabled(true),
			clickhouse.WithServerMetrics(false), // Disable for now to avoid callback complexity
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	fmt.Println("✅ Connected to ClickHouse")
	fmt.Println("✅ Tracing enabled - sending to Jaeger")
	fmt.Println()
	fmt.Println("Running queries...")
	fmt.Println()

	ctx := context.Background()

	// Example 1: Simple query
	fmt.Println("1. Simple COUNT query...")
	var count uint64
	row := conn.QueryRow(ctx, "SELECT COUNT() FROM system.numbers LIMIT 1000000")
	if err := row.Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   ✓ Result: %d\n", count)

	// Example 2: Create table
	fmt.Println("\n2. Creating test table...")
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS otel_demo"); err != nil {
		log.Fatal(err)
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE otel_demo (
			id UInt64,
			name String,
			value Float64,
			timestamp DateTime
		) ENGINE = Memory
	`); err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Table created")

	// Example 3: Batch insert
	fmt.Println("\n3. Batch inserting 1000 rows...")
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO otel_demo")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		if err := batch.Append(
			uint64(i),
			fmt.Sprintf("name_%d", i),
			float64(i)*1.5,
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Inserted 1000 rows")

	// Example 4: Query with results
	fmt.Println("\n4. Querying data...")
	rows, err := conn.Query(ctx, "SELECT id, name, value FROM otel_demo WHERE id < 10 ORDER BY id")
	if err != nil {
		log.Fatal(err)
	}

	rowCount := 0
	for rows.Next() {
		var id uint64
		var name string
		var value float64
		if err := rows.Scan(&id, &name, &value); err != nil {
			rows.Close()
			log.Fatal(err)
		}
		rowCount++
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   ✓ Read %d rows\n", rowCount)

	// Example 5: Aggregation query
	fmt.Println("\n5. Running aggregation query...")
	var avg float64
	var total uint64
	row = conn.QueryRow(ctx, "SELECT avg(value), count() FROM otel_demo")
	if err := row.Scan(&avg, &total); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   ✓ Average: %.2f, Total: %d\n", avg, total)

	// Example 6: Ping
	fmt.Println("\n6. Pinging server...")
	if err := conn.Ping(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Server is alive")

	// Cleanup
	fmt.Println("\n7. Cleaning up...")
	if err := conn.Exec(ctx, "DROP TABLE otel_demo"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("   ✓ Table dropped")

	fmt.Println("\n" + "=".repeat(50))
	fmt.Println("✅ All operations completed successfully!")
	fmt.Println("=".repeat(50))
	fmt.Println()
	fmt.Println("📊 View traces in Jaeger UI:")
	fmt.Println("   http://localhost:16686")
	fmt.Println()
	fmt.Println("In Jaeger:")
	fmt.Println("1. Select service: 'clickhouse-go-demo'")
	fmt.Println("2. Click 'Find Traces'")
	fmt.Println("3. Click on any trace to see details")
	fmt.Println()
	fmt.Println("What to look for:")
	fmt.Println("- Operation names: clickhouse.query, clickhouse.exec, etc.")
	fmt.Println("- Span duration: Total client-side time")
	fmt.Println("- Attributes: db.statement, db.operation, etc.")
	fmt.Println("- Timeline: When each operation occurred")
	fmt.Println()

	// Give time for traces to be exported
	fmt.Println("Waiting 3 seconds for traces to be exported...")
	time.Sleep(3 * time.Second)
	fmt.Println("Done!")
}

func initTracer() (func(), error) {
	ctx := context.Background()

	// Create gRPC connection to Jaeger
	conn, err := grpc.NewClient("localhost:4317",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to Jaeger: %w", err)
	}

	// Create OTLP trace exporter
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP trace exporter: %w", err)
	}

	// Create tracer provider with resource attributes
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("clickhouse-go-demo"),
			semconv.ServiceVersionKey.String("1.0.0"),
			attribute.String("environment", "demo"),
			attribute.String("demo.name", "clickhouse-otel-tracing"),
		)),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Return shutdown function
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}, nil
}

// Helper for string repetition (for formatting)
type stringRepeater string

func (s stringRepeater) repeat(n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += string(s)
	}
	return result
}

var repeat = stringRepeater("=").repeat
