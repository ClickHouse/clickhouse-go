# Long-Running Queries and Timeouts

## The Problem

When executing a long-running query (such as `INSERT FROM SELECT` or complex analytical queries) that does not continuously send or receive data, the client sends the statement and waits for a response. If a load balancer sits between the client and ClickHouse server with an idle connection timeout shorter than the query execution time, the load balancer will close the connection before the query finishes—even though the query is actively running on the server.

This is particularly common with:
- Cloud load balancers (often 60-120 second idle timeouts)
- HTTP proxies with connection timeouts
- Long-running `INSERT FROM SELECT` operations
- Complex analytical queries on large datasets

## Symptoms

The clearest symptom is a **connection timeout or "EOF" error** even though the query succeeded on the server. To confirm:

1. Note the `query_id` from the failed request (see "Fire-and-Forget Approach" below for how to set one).
2. Check `system.query_log` on your ClickHouse server:

```sql
SELECT type, query_duration_ms
FROM system.query_log
WHERE query_id = '<your-query-id>'
ORDER BY event_time DESC
LIMIT 5
```

3. If you see a `QueryFinish` row with `query_duration_ms` less than your `ReadTimeout`, the query completed successfully—the connection was dropped before the response arrived.

---

## Solution 1: Configure ClickHouse Progress Headers (HTTP Protocol Only)

When using the HTTP protocol, you can configure ClickHouse to periodically send progress information via HTTP headers. This creates network activity that prevents load balancers from treating the connection as idle.

### How It Works

ClickHouse sends `X-ClickHouse-Progress` headers at regular intervals during query execution, keeping the HTTP connection active from the load balancer's perspective.

### Configuration

**Step 1.** Estimate your maximum query execution time and set `ReadTimeout` safely above it:

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Protocol: clickhouse.HTTP,
    Addr:     []string{"localhost:8123"},
    Settings: clickhouse.Settings{
        // Enable progress headers
        "send_progress_in_http_headers": 1,
        // Send headers every 110 seconds (adjust based on your LB timeout)
        // If your load balancer has a 120s idle timeout, use ~110000 (110s)
        "http_headers_progress_interval_ms": "110000",
    },
    // Allow up to 400 seconds for query completion
    ReadTimeout: 400 * time.Second,
})
```

**Step 2.** Execute your query normally:

```go
ctx := context.Background()
err = conn.Exec(ctx, `INSERT INTO my_table SELECT * FROM source_table`)
if err != nil {
    log.Fatal(err)
}
```

### Important Notes

- **HTTP protocol only**: Progress headers work with `Protocol: clickhouse.HTTP`, not the native protocol
- **Interval tuning**: Set `http_headers_progress_interval_ms` to a value 10-20% below your load balancer's idle timeout
- **String value**: Use a string value (e.g., `"110000"`) because ClickHouse settings use UInt64, which can exceed Go's int range
- **Trade-off**: The HTTP connection remains open for the query's full duration—network interruptions will fail the operation

### Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
    conn, err := clickhouse.Open(&clickhouse.Options{
        Protocol: clickhouse.HTTP,
        Addr:     []string{"localhost:8123"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
        Settings: clickhouse.Settings{
            // Enable progress in HTTP headers to keep connection alive
            "send_progress_in_http_headers":         1,
            "http_headers_progress_interval_ms":     "110000", // 110 seconds
        },
        // Allow query to run for up to 10 minutes
        ReadTimeout: 10 * time.Minute,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ctx := context.Background()

    // Long-running query example
    err = conn.Exec(ctx, `
        INSERT INTO destination_table
        SELECT *
        FROM large_source_table
        WHERE complex_condition = true
    `)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Println("Long-running query completed successfully")
}
```

---

## Solution 2: Fire-and-Forget with Server-Side Polling (More Resilient)

For mutations and `INSERT FROM SELECT` operations, you can deliberately disconnect after the server receives the query, then poll `system.query_log` until completion. This reduces exposure to network errors from "the entire query duration" to just "a short handshake phase."

**Important**: This approach only works for operations that continue server-side after disconnection (mutations, inserts). SELECT queries will be cancelled.

### How It Works

1. Generate a `query_id` on the client side
2. Start the query but don't wait for completion
3. Poll `system.query_log` to verify the server received it
4. Poll until the query finishes
5. Check the final status

### Implementation

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/google/uuid"
)

func main() {
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Generate unique query ID
    queryID := uuid.New().String()

    // Start the long-running mutation
    ctx := clickhouse.Context(context.Background(),
        clickhouse.WithQueryID(queryID),
    )

    // Execute query in background (intentionally not waiting)
    go func() {
        err := conn.Exec(ctx, `
            INSERT INTO destination_table
            SELECT * FROM large_source_table
        `)
        if err != nil {
            log.Printf("Background query error (may be ignored if intentional): %v", err)
        }
    }()

    // Wait for query to appear in system.query_log
    if err := waitForQueryStart(conn, queryID, 30*time.Second); err != nil {
        log.Fatal("Query never started:", err)
    }

    fmt.Println("Query started, disconnecting client...")

    // Now poll until completion
    if err := waitForQueryComplete(conn, queryID, 10*time.Minute); err != nil {
        log.Fatal("Query failed or timeout:", err)
    }

    fmt.Println("Query completed successfully")
}

func waitForQueryStart(conn clickhouse.Conn, queryID string, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        var exists uint8
        err := conn.QueryRow(context.Background(), `
            SELECT COUNT(*) > 0
            FROM system.query_log
            WHERE query_id = $1
        `, queryID).Scan(&exists)

        if err == nil && exists == 1 {
            return nil
        }
        time.Sleep(1 * time.Second)
    }
    return fmt.Errorf("timeout waiting for query to start")
}

func waitForQueryComplete(conn clickhouse.Conn, queryID string, timeout time.Duration) error {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        var queryType string
        err := conn.QueryRow(context.Background(), `
            SELECT type
            FROM system.query_log
            WHERE query_id = $1 AND type != 'QueryStart'
            ORDER BY event_time DESC
            LIMIT 1
        `, queryID).Scan(&queryType)

        if err == nil {
            if queryType == "QueryFinish" {
                return nil
            }
            return fmt.Errorf("query failed with type: %s", queryType)
        }
        time.Sleep(2 * time.Second)
    }
    return fmt.Errorf("timeout waiting for query completion")
}
```

### Trade-offs

| Aspect | Progress Headers | Fire-and-Forget + Polling |
|--------|------------------|---------------------------|
| Complexity | Low | Medium |
| Network resilience | Lower (connection held open) | Higher (disconnect early) |
| Requires `system.query_log` access | No | Yes |
| Works for SELECT queries | Yes | No (only mutations) |
| Protocol support | HTTP only | Native and HTTP |

---

## Solution 3: Increase ReadTimeout (Simple but Limited)

For simpler cases where you have no load balancer or know your query won't exceed the timeout, just increase `ReadTimeout`:

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"localhost:9000"},
    Auth: clickhouse.Auth{
        Database: "default",
        Username: "default",
        Password: "",
    },
    // Increase read timeout to 10 minutes
    ReadTimeout: 10 * time.Minute,
})
```

**Warning**: This doesn't solve the load balancer idle timeout problem. If a load balancer sits between client and server with a 120-second idle timeout, increasing `ReadTimeout` to 10 minutes won't help—the LB will still close the connection after 120 seconds of inactivity.

---

## Native Protocol Progress Callbacks

When using the native protocol (`clickhouse.Open` without `Protocol: clickhouse.HTTP`), you can receive progress updates via a callback function. These are sent over the native TCP protocol, not HTTP headers:

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    ctx := clickhouse.Context(context.Background(),
        clickhouse.WithProgress(func(p *clickhouse.Progress) {
            fmt.Printf("Progress: %d rows, %d bytes, %d total rows\n",
                p.Rows, p.Bytes, p.TotalRows)
        }),
    )

    rows, err := conn.Query(ctx, "SELECT number FROM numbers(10000000)")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        // Process rows
    }
}
```

**Note**: Native protocol progress callbacks don't solve the load balancer timeout issue—they're informational only. Use HTTP progress headers (Solution 1) for that use case.

---

## Choosing the Right Approach

| Use Case | Recommended Solution |
|----------|---------------------|
| HTTP protocol + load balancer with idle timeout | Solution 1: Progress Headers |
| Long INSERT FROM SELECT, need resilience | Solution 2: Fire-and-Forget + Polling |
| Direct connection, no load balancer | Solution 3: Increase ReadTimeout |
| Want progress updates (native protocol) | Use `WithProgress` callback |
| SELECT queries through load balancer | Solution 1: Progress Headers (HTTP only) |

---

## Additional Resources

- [ClickHouse Settings Documentation](https://clickhouse.com/docs/en/operations/settings/settings)
- [Async Insert Documentation](https://clickhouse.com/docs/en/optimize/asynchronous-inserts)
- [Query Execution Progress](https://clickhouse.com/docs/en/interfaces/http#response-buffering)
