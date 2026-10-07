package issues

import (
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// countingConn wraps net.Conn and records the first Close() call so a test can
// verify every dialed socket is eventually closed (i.e. not leaked).
type countingConn struct {
	net.Conn
	closedOnce sync.Once
	closed     *atomic.Int32
}

func (c *countingConn) Close() error {
	c.closedOnce.Do(func() {
		c.closed.Add(1)
	})
	return c.Conn.Close()
}

// Test1831 is a regression test for https://github.com/ClickHouse/clickhouse-go/issues/1831.
// It exercises the race between concurrent release() calls and Close(), verifying that
// every dialed TCP connection is closed when the pool shuts down mid-flight.
//
// Before the fix, two bugs let TCP connections escape cleanup:
//  1. Close() drained the idle pool before marking the client closed, so a concurrent
//     release() could Put() a connection back into an already-drained pool.
//  2. connPool.Put() dropped the connection without closing it when the pool was closed.
//
// The leak counter below (opened vs closed via opts.DialContext) fails on the pre-fix
// code: connections handed to Put() after shutdown were neither pooled nor closed.
func Test1831(t *testing.T) {
	for _, useHTTP := range []bool{false, true} {
		protocol := "native"
		if useHTTP {
			protocol = "http"
		}
		t.Run(protocol, func(t *testing.T) {
			runConnPoolShutdownLeakCheck(t, useHTTP)
		})
	}
}

func runConnPoolShutdownLeakCheck(t *testing.T, useHTTP bool) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	// Small pool so connections cycle through the idle pool frequently,
	// making the Close/release race more likely to be triggered.
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse_tests.TestClientDefaultSettings(testEnv), useHTTP)
	opts.MaxOpenConns = 5
	opts.MaxIdleConns = 5

	var (
		opened atomic.Int32
		closed atomic.Int32
	)

	// DialContext bypasses opts.TLS for the native protocol, so the TLS handshake has to
	// be performed here. The HTTP transport does its own handshake for https URLs, so it
	// must receive the raw socket.
	opts.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
		raw, err := (&net.Dialer{Timeout: opts.DialTimeout}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		opened.Add(1)
		conn := &countingConn{Conn: raw, closed: &closed}
		if opts.Protocol == clickhouse.Native && opts.TLS != nil {
			cfg := opts.TLS.Clone()
			if cfg.ServerName == "" {
				host, _, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					host = addr
				}
				cfg.ServerName = host
			}
			tlsConn := tls.Client(conn, cfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
		return conn, nil
	}

	conn, err := clickhouse_tests.GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	const workers = 20
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ready := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			for j := 0; j < 10; j++ {
				_ = conn.Ping(ctx) // errors expected once Close() is called
			}
		}()
	}

	close(ready)

	// Close() while queries are in flight: must not panic or leak connections, including
	// any connection dialed between the closed check in acquire() and the drain.
	require.NoError(t, conn.Close())

	wg.Wait()

	// After Close(), operations must return an error promptly, not hang.
	require.Error(t, conn.Ping(context.Background()), "Ping after Close() should return an error")

	// Every dialed socket must have been released and closed by now. Before the fix, a
	// connection handed to Put() after the pool shut down was silently dropped, leaving
	// opened > closed.
	require.Equal(t, opened.Load(), closed.Load(),
		"opened %d TCP connections, closed %d: connection leaked during pool shutdown", opened.Load(), closed.Load())
}
