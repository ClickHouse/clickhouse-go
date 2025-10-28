# OpenTelemetry Tracing Demo

This directory contains examples demonstrating OpenTelemetry tracing with clickhouse-go.

## Quick Start (2 minutes)

### Option 1: Using Docker Compose (Easiest)

```bash
# From the repository root
cd /home/kavi/src/clickhouse-go

# Start ClickHouse and Jaeger
docker-compose -f docker-compose.otel-demo.yml up -d

# Wait for services to be ready (about 10 seconds)
sleep 10

# Run the example
cd examples/clickhouse_api
go run otel_tracing_jaeger.go

# Open Jaeger UI in your browser
# http://localhost:16686
# Select service: "clickhouse-go-demo" and click "Find Traces"

# When done, stop services
cd ../..
docker-compose -f docker-compose.otel-demo.yml down
```

### Option 2: Using Docker Directly

```bash
# Start ClickHouse
docker run -d \
  --name clickhouse-otel \
  -p 9000:9000 \
  -p 8123:8123 \
  clickhouse/clickhouse-server:latest

# Start Jaeger
docker run -d \
  --name jaeger-otel \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -e COLLECTOR_OTLP_ENABLED=true \
  jaegertracing/all-in-one:latest

# Wait a moment for services to start
sleep 5

# Run the example
cd examples/clickhouse_api
go run otel_tracing_jaeger.go

# Open Jaeger UI: http://localhost:16686

# Cleanup
docker stop clickhouse-otel jaeger-otel
docker rm clickhouse-otel jaeger-otel
```

### Option 3: Stdout Tracing (No Jaeger Required)

If you just want to see traces in your terminal:

```bash
# Start only ClickHouse
docker run -d --name clickhouse -p 9000:9000 clickhouse/clickhouse-server

# Run the stdout example
cd examples/clickhouse_api
go run otel_tracing.go

# You'll see JSON trace output in your terminal

# Cleanup
docker stop clickhouse && docker rm clickhouse
```

## What You'll See in Jaeger

### Service View
- Service name: `clickhouse-go-demo`
- Operations: `clickhouse.query`, `clickhouse.exec`, `clickhouse.prepare_batch`, etc.

### Trace Timeline
Each trace shows:
- **Duration**: Total client-side time (includes network, serialization, etc.)
- **Start time**: When the operation began
- **Operation name**: Type of database operation

### Span Details
Click on any span to see attributes:
- `db.system`: "clickhouse"
- `db.operation`: "query", "exec", "prepare_batch", etc.
- `db.statement`: The actual SQL query
- `db.server.address`: ClickHouse server address
- `db.clickhouse.protocol`: "native" or "http"
- `db.clickhouse.server.elapsed_ns`: Server-side execution time (when available)

## Understanding the Metrics

### Client vs Server Time

```
Example Trace:
┌─────────────────────────────────────────────┐
│ clickhouse.query                            │
│ Duration: 150ms (Total client time)        │
│                                             │
│ Attributes:                                 │
│  - db.statement: SELECT COUNT(*)...        │
│  - db.clickhouse.server.elapsed_ns: 50ms   │
└─────────────────────────────────────────────┘

Analysis:
- Total time: 150ms
- Server time: 50ms (33%)
- Client overhead: 100ms (67%)
  ├─ Network latency
  ├─ Serialization/deserialization
  ├─ Connection pool wait
  └─ Result streaming
```

### Performance Insights

By comparing these times, you can identify:

1. **Network issues**: Large difference between total and server time
2. **Query performance**: High server time indicates slow query
3. **Serialization overhead**: Time spent encoding/decoding data
4. **Connection pooling**: Time waiting for available connection

## Examples Included

### 1. `otel_tracing.go`
Basic example using stdout exporter:
- Simple queries
- Parameterized queries
- Batch inserts
- Progress tracking

### 2. `otel_tracing_jaeger.go`
Full example with Jaeger visualization:
- Multiple operation types
- Creates test table
- Batch inserts
- Queries with results
- Aggregations
- Cleanup operations

## Customizing Traces

### Add Custom Attributes

```go
import "go.opentelemetry.io/otel/trace"

// Get current span from context
span := trace.SpanFromContext(ctx)

// Add custom attributes
span.SetAttributes(
    attribute.String("user.id", "12345"),
    attribute.String("query.type", "analytics"),
    attribute.Int64("dataset.size", 1000000),
)
```

### Create Custom Spans

```go
ctx, span := otel.Tracer("my-app").Start(ctx, "my-operation")
defer span.End()

// Your code here
result, err := conn.Query(ctx, "...")
```

### Add Events

```go
span := trace.SpanFromContext(ctx)
span.AddEvent("Cache miss", trace.WithAttributes(
    attribute.String("cache.key", "user:123"),
))
```

## Troubleshooting

### "connection refused" when connecting to Jaeger

```bash
# Check if Jaeger is running
docker ps | grep jaeger

# Check Jaeger logs
docker logs jaeger-otel

# Try curling the endpoint
curl http://localhost:4317
```

### "connection refused" when connecting to ClickHouse

```bash
# Check if ClickHouse is running
docker ps | grep clickhouse

# Try connecting with clickhouse-client
docker exec clickhouse-otel clickhouse-client --query "SELECT 1"
```

### No traces appearing in Jaeger

1. Check that tracing is enabled in your code:
   ```go
   OpenTelemetryOptions: []clickhouse.OtelOption{
       clickhouse.WithOtelEnabled(true),
   }
   ```

2. Make sure to wait a few seconds for traces to export:
   ```go
   time.Sleep(2 * time.Second)
   ```

3. Check service name in Jaeger UI matches your code

### "cannot find package" errors

```bash
# Install required dependencies
cd examples/clickhouse_api
go get go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc
go get google.golang.org/grpc
go mod tidy
```

## Advanced: Production Setup

For production use:

1. **Use sampling** to reduce trace volume:
   ```go
   sdktrace.WithSampler(sdktrace.TraceIDRatioBased(0.1)) // 10%
   ```

2. **Use a proper backend**:
   - Jaeger with Cassandra/Elasticsearch storage
   - Grafana Tempo
   - AWS X-Ray
   - Datadog APM
   - New Relic

3. **Configure resource limits**:
   ```go
   sdktrace.WithBatcher(exporter,
       sdktrace.WithMaxQueueSize(2048),
       sdktrace.WithMaxExportBatchSize(512),
   )
   ```

4. **Add service info**:
   ```go
   resource.NewWithAttributes(
       semconv.ServiceNameKey.String("my-service"),
       semconv.ServiceVersionKey.String("v1.2.3"),
       semconv.DeploymentEnvironmentKey.String("production"),
   )
   ```

## Resources

- **Full Documentation**: See `../../OTEL_TRACING.md`
- **Quick Start Guide**: See `../../QUICKSTART_TRACING.md`
- **Implementation Details**: See `../../OTEL_IMPLEMENTATION_SUMMARY.md`
- **OpenTelemetry Docs**: https://opentelemetry.io/docs/
- **Jaeger Docs**: https://www.jaegertracing.io/docs/
