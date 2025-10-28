# OpenTelemetry Trace Visualization Guide

This guide shows you what to expect when viewing traces in Jaeger or other visualization tools.

## Trace Timeline View

When you open Jaeger and search for traces, you'll see something like this:

```
╔══════════════════════════════════════════════════════════════════╗
║  Jaeger UI - Traces for service: clickhouse-go-demo             ║
╠══════════════════════════════════════════════════════════════════╣
║                                                                  ║
║  Trace ID: abc123...  Duration: 150ms  Spans: 1  [2025-10-24]  ║
║  ▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  clickhouse.query          ║
║                                                                  ║
║  Trace ID: def456...  Duration: 5ms   Spans: 1  [2025-10-24]   ║
║  ▓░░  clickhouse.exec                                           ║
║                                                                  ║
║  Trace ID: ghi789...  Duration: 200ms Spans: 1  [2025-10-24]   ║
║  ▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░  clickhouse.prepare_batch      ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
```

## Single Trace Detail View

Click on any trace to see detailed information:

```
╔═══════════════════════════════════════════════════════════════════════╗
║  Trace: abc123def456...                                               ║
║  Service: clickhouse-go-demo                                          ║
║  Duration: 150.5ms                                                    ║
║  Spans: 1                                                             ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  Timeline (0ms ────────────────────────────────────────── 150ms)     ║
║                                                                       ║
║  clickhouse.query                                                     ║
║  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  150.5ms      ║
║                                                                       ║
╚═══════════════════════════════════════════════════════════════════════╝

▼ Span Details: clickhouse.query

┌─────────────────────────────────────────────────────────────────┐
│ Timing                                                          │
├─────────────────────────────────────────────────────────────────┤
│ Start time:   2025-10-24T14:30:00.123456Z                      │
│ Duration:     150.5ms                                           │
│ Self time:    150.5ms                                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Tags (Attributes)                                               │
├─────────────────────────────────────────────────────────────────┤
│ db.system                  clickhouse                           │
│ db.operation               query                                │
│ db.statement               SELECT COUNT() FROM system.numbers...│
│ db.server.address          localhost:9000                       │
│ db.clickhouse.protocol     native                               │
│ db.clickhouse.rows         1000000                              │
│ db.clickhouse.bytes        8000000                              │
│ db.clickhouse.server.elapsed_ns   50000000  ← Server time!     │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ Process                                                         │
├─────────────────────────────────────────────────────────────────┤
│ service.name               clickhouse-go-demo                   │
│ service.version            1.0.0                                │
│ environment                demo                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Interpreting the Trace

### Key Metrics to Analyze

```
╔═══════════════════════════════════════════════════════════════════╗
║  Span: clickhouse.query                                           ║
║  Duration: 150ms (Total Client Time)                              ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║  ┌─────────────────────────────────────────────────────────────┐ ║
║  │                   150ms (Total Time)                        │ ║
║  │ ┌─────────────────────────────────────────────────────────┐ │ ║
║  │ │     Client Overhead (100ms)       │ Server (50ms)       │ │ ║
║  │ ├───────────────────────────────────┼─────────────────────┤ │ ║
║  │ │ • Network latency      ~40ms      │ • Query execution   │ │ ║
║  │ │ • Serialization        ~30ms      │ • Data processing   │ │ ║
║  │ │ • Connection pool      ~15ms      │ • Block generation  │ │ ║
║  │ │ • Deserialization      ~15ms      │                     │ │ ║
║  │ └───────────────────────────────────┴─────────────────────┘ │ ║
║  └─────────────────────────────────────────────────────────────┘ ║
║                                                                   ║
║  Breakdown:                                                       ║
║  ✓ Server time:      50ms  (33%) ← From db.clickhouse.server... ║
║  ✓ Client overhead: 100ms  (67%) ← Calculated: 150ms - 50ms     ║
║                                                                   ║
╚═══════════════════════════════════════════════════════════════════╝
```

## Real-World Examples

### Example 1: Fast Query, Slow Network

```
Operation: SELECT COUNT(*) FROM small_table

Trace shows:
┌────────────────────────────────────────────────────────┐
│ clickhouse.query                           Duration: 120ms │
│ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓        │
└────────────────────────────────────────────────────────┘

Attributes:
├─ db.clickhouse.server.elapsed_ns: 5000000 (5ms)
└─ db.clickhouse.rows: 1

Analysis:
✓ Server time:      5ms   (4%)  ← Query is fast
✗ Client overhead: 115ms  (96%) ← High network latency!

Recommendation:
→ Check network connection
→ Consider using a server closer to your application
→ Enable compression to reduce data transfer time
```

### Example 2: Slow Query, Fast Network

```
Operation: SELECT * FROM huge_table WHERE complex_condition

Trace shows:
┌────────────────────────────────────────────────────────┐
│ clickhouse.query                          Duration: 5000ms │
│ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓   │
└────────────────────────────────────────────────────────┘

Attributes:
├─ db.clickhouse.server.elapsed_ns: 4800000000 (4800ms)
├─ db.clickhouse.rows: 10000000
└─ db.clickhouse.bytes: 800000000

Analysis:
✗ Server time:     4800ms (96%) ← Query is slow
✓ Client overhead:  200ms  (4%) ← Network is fine

Recommendation:
→ Optimize the query (add WHERE clauses)
→ Add indexes
→ Use materialized views
→ Consider query rewriting
```

### Example 3: Batch Insert

```
Operation: INSERT INTO table VALUES (...)

Trace shows:
┌────────────────────────────────────────────────────────┐
│ clickhouse.prepare_batch                 Duration: 50ms   │
│ ▓▓▓▓▓▓                                                  │
│                                                         │
│ clickhouse.exec (batch.Send)            Duration: 300ms  │
│ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓    │
└────────────────────────────────────────────────────────┘

Attributes (batch.Send):
├─ db.clickhouse.server.elapsed_ns: 250000000 (250ms)
├─ db.clickhouse.progress.wrote_rows: 100000
└─ db.clickhouse.progress.wrote_bytes: 8000000

Analysis:
✓ Prepare time:      50ms  (batch preparation)
✓ Server write:     250ms  (83% - actual insert)
✓ Client overhead:   50ms  (17% - serialization)

Total time: 350ms for 100,000 rows = 3.5µs per row

Recommendation:
→ Performance is good for batch inserts
→ Consider larger batches if possible
```

## Jaeger UI Navigation Guide

### 1. Service View (Homepage)

```
┌─────────────────────────────────────────────────────────┐
│ Jaeger UI                                    [Settings] │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ Service: [clickhouse-go-demo ▼]                        │
│                                                         │
│ Lookback: [Last Hour ▼]                                │
│                                                         │
│ [Find Traces]                                           │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**Steps:**
1. Select your service from dropdown
2. Adjust time range if needed
3. Click "Find Traces"

### 2. Trace List View

```
┌─────────────────────────────────────────────────────────┐
│ Search Results: 15 traces                               │
├─────────────────────────────────────────────────────────┤
│                                                         │
│ [Filter by operation ▼] [Min/Max Duration]             │
│                                                         │
│ ┌─ clickhouse.query              150ms   [14:30:00] ─┐ │
│ │  ▓▓▓▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░                  1s │ │
│ └───────────────────────────────────────────────────┘ │
│                                                         │
│ ┌─ clickhouse.exec                5ms   [14:30:01] ──┐ │
│ │  ▓░░                                             1s │ │
│ └───────────────────────────────────────────────────┘ │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

**What you see:**
- Each row is one trace (one operation)
- Bar length = duration (longer = slower)
- Click any row to see details

### 3. Trace Detail View

```
┌─────────────────────────────────────────────────────────┐
│ ← Back to Search                                        │
├─────────────────────────────────────────────────────────┤
│ Trace Timeline                                          │
│                                                         │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ clickhouse.query                          150.5ms   │ │
│ │ ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓   │ │
│ │                                                     │ │
│ │ ▼ Tags                                              │ │
│ │   db.system: clickhouse                            │ │
│ │   db.operation: query                              │ │
│ │   db.statement: SELECT COUNT()...                  │ │
│ │   db.clickhouse.server.elapsed_ns: 50000000        │ │
│ │                                                     │ │
│ └─────────────────────────────────────────────────────┘ │
│                                                         │
│ [JSON] [Download]                                       │
└─────────────────────────────────────────────────────────┘
```

**What to click:**
- `▼ Tags` - See all attributes
- `[JSON]` - Export trace data
- Span bar - Select different spans

## Comparing Multiple Traces

Use the "Compare Traces" feature to analyze performance differences:

```
╔═══════════════════════════════════════════════════════════╗
║  Comparing 3 traces                                       ║
╠═══════════════════════════════════════════════════════════╣
║                                                           ║
║  Trace 1: SELECT ... WHERE id = 1                         ║
║  ▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  50ms             ║
║                                                           ║
║  Trace 2: SELECT ... WHERE id IN (1,2,3)                  ║
║  ▓▓▓▓▓▓░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  120ms            ║
║                                                           ║
║  Trace 3: SELECT ... (full table scan)                    ║
║  ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓  5000ms            ║
║                                                           ║
║  Analysis: Adding index reduced time by 99%!             ║
╚═══════════════════════════════════════════════════════════╝
```

## Tips for Better Visualization

### 1. Name your operations clearly

```go
// Instead of generic names
ctx, span := tracer.Start(ctx, "database_op")

// Use descriptive names
ctx, span := tracer.Start(ctx, "fetch_user_orders")
```

### 2. Add meaningful attributes

```go
span.SetAttributes(
    attribute.String("user.id", userID),
    attribute.String("table.name", "orders"),
    attribute.Int64("result.count", count),
)
```

### 3. Use consistent naming

All clickhouse-go operations use the pattern:
- `clickhouse.query`
- `clickhouse.exec`
- `clickhouse.prepare_batch`

Follow similar patterns for your custom spans.

## Grafana Tempo Alternative

If using Grafana Tempo instead of Jaeger:

```
┌─────────────────────────────────────────────────────────┐
│ Grafana - Explore                                       │
├─────────────────────────────────────────────────────────┤
│ Data source: [Tempo ▼]                                  │
│                                                         │
│ Query: {service.name="clickhouse-go-demo"}              │
│                                                         │
│ [Run query]                                             │
│                                                         │
│ Results:                                                │
│ ┌───────────────────────────────────────────────────┐   │
│ │ Trace ID │ Duration │ Spans │ Timestamp          │   │
│ ├───────────────────────────────────────────────────┤   │
│ │ abc123   │ 150ms    │ 1     │ 2025-10-24 14:30  │   │
│ │ def456   │ 5ms      │ 1     │ 2025-10-24 14:30  │   │
│ └───────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────┘
```

Similar information, different UI layout.

## Summary

The key things to look for in any tracing visualization:

✅ **Operation name** - What type of database operation
✅ **Total duration** - Full client-side time
✅ **Server elapsed time** - From `db.clickhouse.server.elapsed_ns`
✅ **Client overhead** - Difference between total and server
✅ **Query statement** - The actual SQL executed
✅ **Result metrics** - Rows, bytes processed

This helps you identify:
- Slow queries (high server time)
- Network issues (high client overhead)
- Serialization problems (high client overhead with small data)
- Connection pool issues (delays before operation starts)
