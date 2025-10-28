# OpenTelemetry Tracing in clickhouse-go

This document describes the OpenTelemetry tracing instrumentation in clickhouse-go, which allows you to measure and analyze the time spent on client-side vs server-side operations.

## Overview

The OpenTelemetry integration provides automatic tracing for all database operations, capturing:

1. **Client-Side Time**:
   - Connection pool acquisition time
   - Query serialization and parameter binding
   - Network transmission time
   - Response deserialization time
   - Result set streaming time

2. **Server-Side Time**:
   - Query execution time (captured from ClickHouse server metrics)
   - Rows processed and bytes transferred
   - Server-side profile information

The difference between total operation time and server execution time reveals network latency and client-side serialization overhead.

## Features

- ✅ Automatic span creation for all database operations (Query, QueryRow, Exec, Batch, AsyncInsert, Ping)
- ✅ Server-side metrics captured as span attributes
- ✅ Support for both Native and HTTP protocols
- ✅ Progress tracking integration
- ✅ ProfileInfo integration for detailed execution metrics
- ✅ Compatible with any OpenTelemetry backend (Jaeger, Zipkin, etc.)
- ✅ Zero overhead when disabled
- ✅ Thread-safe

## Quick Start

### 1. Enable Tracing

```go
import (
    "github.com/ClickHouse/clickhouse-go/v2"
    "go.opentelemetry.io/otel"
)

// Configure your OpenTelemetry tracer provider (example with Jaeger)
// ... (see Full Example below)

// Open connection with tracing enabled
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"localhost:9000"},
    Auth: clickhouse.Auth{
        Database: "default",
        Username: "default",
        Password: "",
    },
    // Enable OpenTelemetry tracing
    OpenTelemetryOptions: []clickhouse.OtelOption{
        clickhouse.WithOtelEnabled(true),
        clickhouse.WithServerMetrics(true), // Capture server-side metrics
    },
})
```

### 2. Use the Connection Normally

Once tracing is enabled, all operations are automatically traced:

```go
ctx := context.Background()

// Query operation - automatically traced
rows, err := conn.Query(ctx, "SELECT * FROM my_table")
// ... process rows

// Exec operation - automatically traced
err = conn.Exec(ctx, "INSERT INTO my_table VALUES (?, ?)", 1, "value")

// Batch operation - automatically traced
batch, err := conn.PrepareBatch(ctx, "INSERT INTO my_table")
batch.Append(1, "value")
err = batch.Send()
```

### 3. View Traces

The spans will be exported to your configured OpenTelemetry backend (Jaeger, Zipkin, etc.) where you can visualize:

- Total operation duration (client time)
- Server execution time (in span attributes)
- Network and serialization overhead (difference between total and server time)

## Configuration Options

### `WithOtelEnabled(enabled bool)`

Enables or disables OpenTelemetry tracing.

```go
clickhouse.WithOtelEnabled(true)  // Enable tracing
clickhouse.WithOtelEnabled(false) // Disable tracing (default)
```

### `WithServerMetrics(capture bool)`

Controls whether to capture server-side metrics (Progress and ProfileInfo) as span attributes.

```go
clickhouse.WithServerMetrics(true)  // Capture server metrics (default)
clickhouse.WithServerMetrics(false) // Skip server metrics
```

### `WithTracerProvider(provider trace.TracerProvider)`

Sets a custom tracer provider instead of using the global provider.

```go
tp := sdktrace.NewTracerProvider(/* ... */)
clickhouse.WithTracerProvider(tp)
```

## Span Attributes

Each span includes the following attributes:

### Standard Attributes

- `db.system`: "clickhouse"
- `db.operation`: Operation type (query, exec, batch, etc.)
- `db.statement`: The SQL query (if applicable)
- `db.server.address`: ClickHouse server address
- `db.clickhouse.protocol`: "native" or "http"

### Server-Side Metrics (when `WithServerMetrics(true)`)

From `ProfileInfo`:
- `db.clickhouse.rows`: Total rows processed
- `db.clickhouse.blocks`: Number of blocks
- `db.clickhouse.bytes`: Bytes processed
- `db.clickhouse.applied_limit`: Whether LIMIT was applied
- `db.clickhouse.rows_before_limit`: Rows before LIMIT

From `Progress`:
- `db.clickhouse.progress.rows`: Progress rows
- `db.clickhouse.progress.bytes`: Progress bytes
- `db.clickhouse.progress.total_rows`: Total rows to process
- `db.clickhouse.progress.written_rows`: Rows written (for inserts)
- `db.clickhouse.progress.written_bytes`: Bytes written (for inserts)
- `db.clickhouse.server.elapsed_ns`: **Server-side execution time in nanoseconds**

## Measuring Client vs Server Time

The key to understanding performance is comparing the span duration with the server elapsed time:

```
Client Time = Span Duration
Server Time = db.clickhouse.server.elapsed_ns attribute
Overhead    = Client Time - Server Time
```

The overhead includes:
- Network round-trip time
- Query serialization/deserialization
- Connection pool wait time
- Result set streaming time

## Full Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ClickHouse/clickhouse-go/v2"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/jaeger"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
    // 1. Initialize OpenTelemetry
    shutdown, err := initTracer()
    if err != nil {
        log.Fatal(err)
    }
    defer shutdown()

    // 2. Open ClickHouse connection with tracing
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
        OpenTelemetryOptions: []clickhouse.OtelOption{
            clickhouse.WithOtelEnabled(true),
            clickhouse.WithServerMetrics(true),
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // 3. Execute queries - automatically traced!
    ctx := context.Background()

    var count uint64
    row := conn.QueryRow(ctx, "SELECT COUNT() FROM system.numbers LIMIT 1000000")
    if err := row.Scan(&count); err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Count: %d\n", count)
}

func initTracer() (func(), error) {
    // Create Jaeger exporter
    exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(
        jaeger.WithEndpoint("http://localhost:14268/api/traces"),
    ))
    if err != nil {
        return nil, err
    }

    // Create tracer provider
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exporter),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceNameKey.String("my-app"),
        )),
    )

    otel.SetTracerProvider(tp)

    return func() {
        if err := tp.Shutdown(context.Background()); err != nil {
            log.Printf("Error shutting down tracer: %v", err)
        }
    }, nil
}
```

## Operation Types

The following operations create spans:

1. **Query** - `clickhouse.query`
   - Full query execution with streaming results
   - Server metrics captured during streaming

2. **QueryRow** - `clickhouse.query_row`
   - Single row query
   - Server metrics captured

3. **Exec** - `clickhouse.exec`
   - DDL/DML execution
   - Server metrics captured

4. **PrepareBatch** - `clickhouse.prepare_batch`
   - Batch preparation
   - Actual send will have server metrics

5. **AsyncInsert** - `clickhouse.async_insert`
   - Async insert operations
   - Server metrics captured

6. **Ping** - `clickhouse.ping`
   - Connection health check

## Best Practices

### 1. Enable Selectively

Only enable tracing in environments where you need observability (staging, production). Disable in development to reduce overhead.

### 2. Use Sampling

For high-throughput applications, use sampling to reduce trace volume:

```go
tp := sdktrace.NewTracerProvider(
    sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1)), // Sample 10% of traces
    // ... other options
)
```

### 3. Add Custom Attributes

You can add custom attributes to the context:

```go
span := trace.SpanFromContext(ctx)
span.SetAttributes(
    attribute.String("user.id", "12345"),
    attribute.String("query.type", "analytics"),
)
```

### 4. Capture Progress for Long Queries

For long-running queries, use progress callbacks to get intermediate server metrics:

```go
ctx = clickhouse.Context(ctx,
    clickhouse.WithProgress(func(p *clickhouse.Progress) {
        log.Printf("Progress: %d rows, elapsed: %d ns",
            p.Rows, p.ElapsedNs)
    }),
)
```

## Performance Impact

When tracing is **disabled** (default):
- Zero overhead - no span creation or attribute recording

When tracing is **enabled**:
- Minimal overhead (~100-500 nanoseconds per operation)
- Asynchronous batch export to backend (no blocking)
- Recommended to use sampling in high-throughput scenarios

## Troubleshooting

### No traces appearing

1. Check that OpenTelemetry is properly initialized:
   ```go
   otel.SetTracerProvider(tp)
   ```

2. Verify tracing is enabled:
   ```go
   OpenTelemetryOptions: []clickhouse.OtelOption{
       clickhouse.WithOtelEnabled(true),
   }
   ```

3. Ensure your exporter is properly configured and reachable

### Server metrics not captured

1. Verify `WithServerMetrics(true)` is set
2. Ensure your ClickHouse server version supports ProfileInfo (most recent versions do)
3. Check that queries actually return progress/profile info (some operations don't)

### Large trace volumes

Use sampling:
```go
sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1))
```

## Integration with Other Tools

### Jaeger

```go
import "go.opentelemetry.io/otel/exporters/jaeger"

exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(
    jaeger.WithEndpoint("http://localhost:14268/api/traces"),
))
```

### Zipkin

```go
import "go.opentelemetry.io/otel/exporters/zipkin"

exporter, err := zipkin.New("http://localhost:9411/api/v2/spans")
```

### OTLP (OpenTelemetry Protocol)

```go
import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

exporter, err := otlptracegrpc.New(ctx,
    otlptracegrpc.WithEndpoint("localhost:4317"),
    otlptracegrpc.WithInsecure(),
)
```

## Examples

See the `examples/clickhouse_api/otel_tracing.go` file for a complete working example that demonstrates:

1. Simple queries with tracing
2. Parameterized queries
3. Batch inserts with metrics
4. Progress tracking
5. Server-side metric capture

Run the example:
```bash
cd examples/clickhouse_api
go run otel_tracing.go
```

## Architecture

The tracing implementation follows these principles:

1. **Non-invasive**: Uses the existing context flow, no API changes required
2. **Opt-in**: Disabled by default, no performance impact when not used
3. **Standards-compliant**: Uses OpenTelemetry semantic conventions for database operations
4. **Comprehensive**: Captures both client and server metrics automatically
5. **Protocol-agnostic**: Works with both Native and HTTP protocols

## Future Enhancements

Potential future improvements:

- [ ] Span events for batch append operations
- [ ] Connection pool metrics as span attributes
- [ ] Automatic query parsing to extract table names
- [ ] Histogram metrics for query durations
- [ ] Support for distributed tracing across microservices

## Contributing

If you encounter issues or have suggestions for improving the tracing integration, please open an issue on GitHub.
