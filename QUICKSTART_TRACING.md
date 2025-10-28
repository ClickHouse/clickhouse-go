# Quick Start: Running and Visualizing OpenTelemetry Traces

This guide walks you through running the OTel tracing example and visualizing the traces.

## Prerequisites

1. Docker (for running ClickHouse and Jaeger)
2. Go 1.21+ installed
3. The clickhouse-go repository cloned

## Option 1: Quick Demo with Stdout (No Setup Required)

The simplest way to see traces is using the built-in stdout exporter:

### Step 1: Start ClickHouse

```bash
docker run -d \
  --name clickhouse-server \
  -p 9000:9000 \
  -p 8123:8123 \
  clickhouse/clickhouse-server:latest
```

### Step 2: Run the Example

```bash
cd examples/clickhouse_api
go run otel_tracing.go
```

You'll see trace output directly in your terminal showing:
- Span names (e.g., `clickhouse.query`)
- Span duration (total client time)
- Attributes including `db.clickhouse.server.elapsed_ns` (server time)

**Example Output:**
```json
{
  "Name": "clickhouse.query_row",
  "SpanContext": { ... },
  "StartTime": "2025-10-24T16:30:00.123456789Z",
  "EndTime": "2025-10-24T16:30:00.234567890Z",
  "Attributes": [
    {"Key": "db.system", "Value": {"Type": "STRING", "Value": "clickhouse"}},
    {"Key": "db.operation", "Value": {"Type": "STRING", "Value": "query_row"}},
    {"Key": "db.statement", "Value": {"Type": "STRING", "Value": "SELECT COUNT() FROM system.numbers LIMIT 1000000"}},
    {"Key": "db.clickhouse.server.elapsed_ns", "Value": {"Type": "INT64", "Value": 1234567}}
  ],
  "Resource": [ ... ]
}
```

**Interpreting the Output:**
- Total time = EndTime - StartTime (e.g., ~111ms)
- Server time = `db.clickhouse.server.elapsed_ns` (e.g., ~1.2ms)
- Client overhead = Total - Server (e.g., ~110ms for network + serialization)

### Step 3: Cleanup

```bash
docker stop clickhouse-server
docker rm clickhouse-server
```

---

## Option 2: Full Visualization with Jaeger (Recommended)

For a proper visualization with timeline views and distributed tracing, use Jaeger.

### Step 1: Start Infrastructure

Create a `docker-compose.yml` file:

```yaml
version: '3'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"

  jaeger:
    image: jaegertracing/all-in-one:latest
    environment:
      - COLLECTOR_OTLP_ENABLED=true
    ports:
      - "16686:16686"  # Jaeger UI
      - "4317:4317"    # OTLP gRPC
      - "4318:4318"    # OTLP HTTP
      - "14268:14268"  # Jaeger collector
```

Start the services:

```bash
docker-compose up -d
```

Or manually:

```bash
# Start ClickHouse
docker run -d \
  --name clickhouse-server \
  --network host \
  clickhouse/clickhouse-server:latest

# Start Jaeger
docker run -d \
  --name jaeger \
  --network host \
  -e COLLECTOR_OTLP_ENABLED=true \
  jaegertracing/all-in-one:latest
```

### Step 2: Create a Modified Example

Create `examples/clickhouse_api/otel_tracing_jaeger.go`:

```go
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
			clickhouse.WithServerMetrics(true),
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	fmt.Println("Running queries with tracing enabled...")
	fmt.Println("Open Jaeger UI at http://localhost:16686")
	fmt.Println()

	// Example 1: Simple query
	fmt.Println("1. Simple query...")
	var count uint64
	row := conn.QueryRow(context.Background(), "SELECT COUNT() FROM system.numbers LIMIT 1000000")
	if err := row.Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("   Result: %d\n", count)

	// Example 2: Create table and batch insert
	fmt.Println("\n2. Batch insert...")
	ctx := context.Background()

	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS otel_demo"); err != nil {
		log.Fatal(err)
	}

	if err := conn.Exec(ctx, `
		CREATE TABLE otel_demo (
			id UInt64,
			name String,
			timestamp DateTime
		) ENGINE = Memory
	`); err != nil {
		log.Fatal(err)
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO otel_demo")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		if err := batch.Append(uint64(i), fmt.Sprintf("name_%d", i), time.Now()); err != nil {
			log.Fatal(err)
		}
	}

	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("   Inserted 1000 rows")

	// Example 3: Query with results
	fmt.Println("\n3. Query with results...")
	rows, err := conn.Query(ctx, "SELECT id, name FROM otel_demo LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		var id uint64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		rowCount++
	}
	fmt.Printf("   Read %d rows\n", rowCount)

	// Cleanup
	if err := conn.Exec(ctx, "DROP TABLE otel_demo"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\n✅ Done! View traces at http://localhost:16686")
	fmt.Println("   Service: clickhouse-go-demo")

	// Give time for traces to be exported
	time.Sleep(2 * time.Second)
}

func initTracer() (func(), error) {
	// Create OTLP exporter
	ctx := context.Background()

	conn, err := grpc.NewClient("localhost:4317",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create exporter: %w", err)
	}

	// Create tracer provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("clickhouse-go-demo"),
			attribute.String("environment", "demo"),
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

### Step 3: Install Dependencies

```bash
cd examples/clickhouse_api
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
go get google.golang.org/grpc
```

### Step 4: Run the Example

```bash
go run otel_tracing_jaeger.go
```

### Step 5: View Traces in Jaeger

1. Open your browser to **http://localhost:16686**
2. In the "Service" dropdown, select **clickhouse-go-demo**
3. Click **Find Traces**

You'll see:
- **Timeline view** showing when each operation started and how long it took
- **Span details** with all attributes (query, server time, etc.)
- **Waterfall view** showing operation hierarchy

### Understanding the Jaeger UI

#### Main View
- Each row is a trace (one complete operation)
- Spans are color-coded
- Duration shows total client time

#### Trace Detail View (click on a trace)
- **Waterfall**: Visual timeline of all operations
- **Span duration**: Total time for that operation
- **Tags**: Click to see all attributes
  - Look for `db.clickhouse.server.elapsed_ns` - this is server time
  - Compare with span duration to see client overhead

#### Key Metrics to Look For

```
Span: clickhouse.query
Duration: 150ms (total client time)

Attributes:
├─ db.system: clickhouse
├─ db.operation: query
├─ db.statement: SELECT * FROM ...
├─ db.server.address: localhost:9000
└─ db.clickhouse.server.elapsed_ns: 50000000 (50ms)

Analysis:
Total time: 150ms
Server time: 50ms
Client overhead: 100ms (network + serialization)
```

### Step 6: Cleanup

```bash
docker-compose down
# or
docker stop clickhouse-server jaeger
docker rm clickhouse-server jaeger
```

---

## Option 3: Grafana + Tempo (Production-Like Setup)

For a more production-like observability stack:

### Docker Compose Setup

```yaml
version: '3'
services:
  clickhouse:
    image: clickhouse/clickhouse-server:latest
    ports:
      - "9000:9000"
      - "8123:8123"

  tempo:
    image: grafana/tempo:latest
    command: [ "-config.file=/etc/tempo.yaml" ]
    volumes:
      - ./tempo.yaml:/etc/tempo.yaml
    ports:
      - "4317:4317"   # OTLP gRPC
      - "3200:3200"   # Tempo

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Admin
    volumes:
      - ./grafana-datasources.yaml:/etc/grafana/provisioning/datasources/datasources.yaml
```

Create `tempo.yaml`:

```yaml
server:
  http_listen_port: 3200

distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317

storage:
  trace:
    backend: local
    local:
      path: /tmp/tempo/traces
```

Create `grafana-datasources.yaml`:

```yaml
apiVersion: 1

datasources:
  - name: Tempo
    type: tempo
    access: proxy
    url: http://tempo:3200
    isDefault: true
```

Start and access Grafana at http://localhost:3000 (login: admin/admin)

---

## Troubleshooting

### No traces appearing in Jaeger

1. Check that Jaeger is running:
   ```bash
   docker ps | grep jaeger
   curl http://localhost:16686
   ```

2. Check that the example can connect:
   ```bash
   # Should not error
   curl http://localhost:4317
   ```

3. Verify ClickHouse is accessible:
   ```bash
   docker ps | grep clickhouse
   ```

4. Check logs:
   ```bash
   docker logs jaeger
   docker logs clickhouse-server
   ```

### Connection refused errors

- Make sure services are on the same network or use `--network host`
- Try using `host.docker.internal` instead of `localhost` on Mac/Windows

### Traces are incomplete

- Add `time.Sleep(2 * time.Second)` before program exits
- Traces are exported asynchronously; give time for export

---

## Understanding the Traces

### What to Look For

1. **Total Latency**: Span duration from Jaeger UI
2. **Server Time**: Look for `db.clickhouse.server.elapsed_ns` attribute
3. **Client Overhead**: Difference between total and server time

### Example Analysis

```
Operation: SELECT COUNT(*) FROM large_table

Trace shows:
├─ clickhouse.query: 500ms total
│  ├─ Attributes:
│  │  └─ db.clickhouse.server.elapsed_ns: 450000000 (450ms)
│
Analysis:
- Total time: 500ms
- Server processing: 450ms (90%)
- Client overhead: 50ms (10%)
  - Network: ~20ms
  - Serialization: ~15ms
  - Connection pool: ~15ms
```

### Performance Optimization Insights

Based on trace analysis:

- **High server time**: Optimize query, add indexes
- **High client overhead**:
  - Check network latency
  - Use connection pooling
  - Enable compression
  - Batch operations

---

## Next Steps

1. **Add custom attributes** to your queries:
   ```go
   span := trace.SpanFromContext(ctx)
   span.SetAttributes(attribute.String("user.id", "12345"))
   ```

2. **Add sampling** for production:
   ```go
   sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1)) // 10%
   ```

3. **Integrate with your APM**:
   - Datadog: Use Datadog exporter
   - New Relic: Use OTLP exporter
   - AWS X-Ray: Use X-Ray exporter

4. **Set up alerts** based on trace durations

---

## Additional Resources

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [ClickHouse Documentation](https://clickhouse.com/docs)
- Full implementation details: See `OTEL_TRACING.md`
