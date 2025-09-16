# SET ROLE Support for ClickHouse Go Driver

This document describes the implementation of SET ROLE support for the ClickHouse Go driver, addressing the feature request in [#1391](https://github.com/ClickHouse/clickhouse-go/discussions/1391) and [#1443](https://github.com/ClickHouse/clickhouse-go/issues/1443).

## Problem Statement

The current clickhouse-go driver uses connection pooling where each operation acquires a connection from the pool, executes the query, and releases it back to the pool. This design makes it impossible to maintain connection state across multiple operations, which is required for features like `SET ROLE`.

## Solution: Session Management

We've implemented a **Session Management** feature that allows users to acquire and hold a connection for multiple operations while maintaining connection state.

### Key Features

1. **Stateful Connections**: Sessions maintain connection state across multiple operations
2. **Resource Management**: Proper connection pool integration with automatic cleanup
3. **Error Handling**: Comprehensive error handling with specific error types
4. **Debug Logging**: Full debug logging support for troubleshooting
5. **Backward Compatibility**: Additive changes that don't break existing code

## API Design

### New Interface: Session

```go
type Session interface {
    // Exec executes a query without returning results
    Exec(ctx context.Context, query string, args ...any) error
    // Query executes a query and returns rows
    Query(ctx context.Context, query string, args ...any) (Rows, error)
    // QueryRow executes a query and returns a single row
    QueryRow(ctx context.Context, query string, args ...any) Row
    // PrepareBatch prepares a batch for insertion
    PrepareBatch(ctx context.Context, query string, opts ...PrepareBatchOption) (Batch, error)
    // Ping checks if the connection is still alive
    Ping(ctx context.Context) error
    // Close releases the session back to the connection pool
    Close() error
}
```

### New Method: AcquireSession

```go
// AcquireSession acquires a connection from the pool and returns a Session
// that maintains connection state for multiple operations
AcquireSession(ctx context.Context) (Session, error)
```

## Usage Examples

### Basic SET ROLE Usage

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
    // Open connection
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
        Settings: clickhouse.Settings{
            "max_execution_time": 60,
        },
        DialTimeout:      time.Second * 30,
        MaxOpenConns:     5,
        MaxIdleConns:     5,
        ConnMaxLifetime:  time.Hour,
        ConnOpenStrategy: clickhouse.ConnOpenInOrder,
        Debug:           false,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Acquire a session for stateful operations
    session, err := conn.AcquireSession(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    defer session.Close()

    // Set role for this session
    err = session.Exec(context.Background(), "SET ROLE admin")
    if err != nil {
        log.Fatal(err)
    }

    // Execute queries with the role applied
    rows, err := session.Query(context.Background(), "SELECT currentUser(), currentRole()")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var user, role string
        err := rows.Scan(&user, &role)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User: %s, Role: %s\n", user, role)
    }
}
```

### Session State Persistence

```go
// Set session variables that persist across operations
err = session.Exec(context.Background(), "SET max_memory_usage = 1000000")
if err != nil {
    log.Fatal(err)
}

// Verify the setting is applied
rows, err := session.Query(context.Background(), 
    "SELECT value FROM system.settings WHERE name = 'max_memory_usage'")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

if rows.Next() {
    var value string
    err := rows.Scan(&value)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Max memory usage: %s\n", value)
}
```

### Multiple Sessions Isolation

```go
// Create multiple sessions - each maintains its own state
session1, err := conn.AcquireSession(context.Background())
if err != nil {
    log.Fatal(err)
}
defer session1.Close()

session2, err := conn.AcquireSession(context.Background())
if err != nil {
    log.Fatal(err)
}
defer session2.Close()

// Set different roles in each session
err = session1.Exec(context.Background(), "SET ROLE admin")
if err != nil {
    log.Fatal(err)
}

err = session2.Exec(context.Background(), "SET ROLE readonly")
if err != nil {
    log.Fatal(err)
}

// Each session maintains its own state
rows1, err := session1.Query(context.Background(), "SELECT currentRole()")
if err != nil {
    log.Fatal(err)
}
defer rows1.Close()

rows2, err := session2.Query(context.Background(), "SELECT currentRole()")
if err != nil {
    log.Fatal(err)
}
defer rows2.Close()

// Verify different roles
if rows1.Next() {
    var role1 string
    rows1.Scan(&role1)
    fmt.Printf("Session 1 role: %s\n", role1)
}

if rows2.Next() {
    var role2 string
    rows2.Scan(&role2)
    fmt.Printf("Session 2 role: %s\n", role2)
}
```

### Error Handling

```go
session, err := conn.AcquireSession(context.Background())
if err != nil {
    log.Fatal(err)
}
defer session.Close()

// Close the session
session.Close()

// These operations will return ErrSessionClosed
err = session.Exec(context.Background(), "SELECT 1")
if err != nil {
    fmt.Printf("Expected error: %v\n", err)
}

_, err = session.Query(context.Background(), "SELECT 1")
if err != nil {
    fmt.Printf("Expected error: %v\n", err)
}
```

## Error Types

The implementation introduces specific error types for better error handling:

```go
var (
    ErrSessionClosed       = errors.New("clickhouse: session is closed")
    ErrSessionNotSupported = errors.New("clickhouse: session operations not supported in this context")
)
```

## Resource Management

Sessions properly integrate with the connection pool:

1. **Acquisition**: Sessions acquire connections from the pool
2. **State Maintenance**: Connections maintain state across operations
3. **Release**: Sessions release connections back to the pool when closed
4. **Cleanup**: Automatic cleanup on session close or error

## Debug Logging

Sessions support comprehensive debug logging:

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    // ... other options ...
    Debug: true,
    Debugf: func(format string, v ...any) {
        log.Printf("[SESSION] "+format, v...)
    },
})
```

Debug output includes:
- Session acquisition and release
- Query execution with SQL
- Error conditions
- Connection state changes

## Testing

Comprehensive tests are provided in `tests/set_role_test.go`:

- Basic session functionality
- SET ROLE operations
- Session state persistence
- Error handling
- Resource management
- Connection pool integration

## Backward Compatibility

This implementation is fully backward compatible:

- No breaking changes to existing APIs
- Sessions are additive functionality
- Existing code continues to work unchanged
- Connection pooling behavior unchanged for non-session operations

## Performance Considerations

- Sessions hold connections longer than regular operations
- Use sessions only when stateful operations are required
- Close sessions promptly to return connections to the pool
- Consider connection pool size when using multiple sessions

## Future Enhancements

Potential future improvements:

1. **Batch Support**: Full batch operation support in sessions
2. **Transaction Integration**: Better integration with database/sql transactions
3. **Session Pooling**: Dedicated session pools for high-throughput scenarios
4. **Configuration Options**: Session-specific configuration options

## Conclusion

This implementation provides a robust, well-tested solution for SET ROLE functionality while maintaining the high standards of the clickhouse-go driver. The design follows established patterns in the codebase and provides a clean, intuitive API for users. 