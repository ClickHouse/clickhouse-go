package issues

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// failOnWriteConn wraps net.Conn and injects a *net.OpError on the next Write call,
// simulating a broken TCP connection (e.g. write deadline exceeded) exactly once.
type failOnWriteConn struct {
	net.Conn
	failNext atomic.Bool
}

func (c *failOnWriteConn) Write(b []byte) (int, error) {
	if c.failNext.CompareAndSwap(true, false) {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: os.ErrDeadlineExceeded}
	}
	return c.Conn.Write(b)
}

// Test1868_SqlConnection_EvictedFromPoolOnSendDataWriteError verifies that a *net.OpError
// returned from a write is correctly detected as a broken connection.
// database/sql must receive driver.ErrBadConn so it can evict the connection and retry
// the operation on a new one transparently to the caller.
func Test1868_SqlConnection_EvictedFromPoolOnSendDataWriteError(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	var (
		latestConn *failOnWriteConn
		dialCount  atomic.Int32
	)

	connector := clickhouse.Connector(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount.Add(1)
			var d net.Dialer
			c, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			latestConn = &failOnWriteConn{Conn: c}
			return latestConn, nil
		},
	})

	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()

	require.NoError(t, db.PingContext(ctx))
	require.Equal(t, int32(1), dialCount.Load(), "setup: one connection dialed")

	// Arm a single write failure on the current connection.
	latestConn.failNext.Store(true)

	// With the fix: the driver detects the *net.OpError as driver.ErrBadConn,
	// database/sql retries on a new connection, and the operation succeeds.
	// Without the fix: the wrapped error is not detected, no retry, error returned.
	_, err = db.ExecContext(ctx, "SELECT 1")
	require.NoError(t, err)

	// With MaxOpenConns(1), a second dial can only happen if the broken connection
	// was evicted — the pool had no other connection to reuse. Demonstrating both
	// eviction and retry in a single assertion.
	require.Equal(t, int32(2), dialCount.Load(), "pool should evict broken connection and dial a new one for the retry")
}
