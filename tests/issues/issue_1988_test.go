package issues

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// corruptResponseConn changes one packet type in a real server response while
// leaving the remaining bytes unread by the decoder.
type corruptResponseConn struct {
	net.Conn
	corrupt *atomic.Bool
}

func (c *corruptResponseConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.corrupt.Swap(false) {
		p[0] = 42
	}
	return n, err
}

func TestIssue1988(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	options := clickhouse_tests.ClientOptionsFromEnv(env, nil, false)
	var corrupt atomic.Bool
	var dials atomic.Int32
	options.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
		var conn net.Conn
		var err error
		if options.TLS != nil {
			conn, err = (&tls.Dialer{Config: options.TLS}).DialContext(ctx, "tcp", addr)
		} else {
			conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr)
		}
		if err != nil {
			return nil, err
		}
		dials.Add(1)
		return &corruptResponseConn{Conn: conn, corrupt: &corrupt}, nil
	}
	db := clickhouse.OpenDB(&options)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))
	require.EqualValues(t, 1, dials.Load())

	corrupt.Store(true)
	var value uint8
	err = db.QueryRowContext(ctx, "SELECT toUInt8(1)").Scan(&value)
	require.ErrorContains(t, err, "unexpected packet 42")
	require.NotErrorIs(t, err, driver.ErrBadConn)
	require.EqualValues(t, 1, dials.Load(), "do not retry the failed query")

	require.NoError(t, db.QueryRowContext(ctx, "SELECT toUInt8(2)").Scan(&value))
	require.EqualValues(t, 2, value, "do not read the previous response")
	require.EqualValues(t, 2, dials.Load(), "the next query needs a fresh connection")

	err = db.QueryRowContext(ctx, "SELECT throwIf(1, 'issue 1988 expected exception')").Scan(&value)
	var exception *clickhouse.Exception
	require.ErrorAs(t, err, &exception)
	require.NoError(t, db.QueryRowContext(ctx, "SELECT toUInt8(3)").Scan(&value))
	require.EqualValues(t, 3, value)
	require.EqualValues(t, 2, dials.Load(), "a fully decoded server exception is safe to reuse")
}
