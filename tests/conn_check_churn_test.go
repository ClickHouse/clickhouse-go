//go:build linux || darwin

// Connection reuse regression suite. Guards against connection churn: healthy
// pooled connections being killed and re-dialed (TCP + handshake + auth),
// whether via isBad()/connCheck() reacting to unread bytes on an idle socket,
// error releases, or pool ordering races. Also covers the inverse failure:
// half-open connections that a socket-level check cannot detect.
//
// Instrumentation:
//   - a counting DialContext records every physical dial the driver makes;
//   - between operations (socket idle) each live connection is peeked with
//     MSG_PEEK|MSG_DONTWAIT — the same signal connCheck() reacts to, but
//     non-destructive, so a positive result can be observed and reported;
//   - a blackhole proxy simulates a middlebox silently dropping idle flows.
package tests

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// connTracker records every connection the driver dials and can inspect the
// kernel receive buffer of live connections without consuming data.
type connTracker struct {
	iter atomic.Int64 // current workload iteration, stamped onto each dial

	mu        sync.Mutex
	dials     int
	dialIters []int64
	conns     []net.Conn
}

func (ct *connTracker) DialContext(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	ct.mu.Lock()
	ct.dials++
	ct.dialIters = append(ct.dialIters, ct.iter.Load())
	ct.conns = append(ct.conns, conn)
	ct.mu.Unlock()
	return conn, nil
}

func (ct *connTracker) Dials() int {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return ct.dials
}

func (ct *connTracker) DialIterations() []int64 {
	ct.mu.Lock()
	defer ct.mu.Unlock()
	return append([]int64(nil), ct.dialIters...)
}

// pendingByConn peeks each tracked connection with MSG_PEEK|MSG_DONTWAIT and
// returns the number of readable bytes for every connection that has any.
// Connections already closed by the driver are skipped. Must only be called
// while no query is in flight, i.e. when every live connection is idle in the
// pool — the exact state connCheck() probes in acquire()/Put().
func (ct *connTracker) pendingByConn(t *testing.T) map[int]int {
	t.Helper()
	ct.mu.Lock()
	defer ct.mu.Unlock()

	pending := make(map[int]int)
	for i, conn := range ct.conns {
		sc, ok := conn.(syscall.Conn)
		require.True(t, ok, "test dialer must produce syscall.Conn")
		raw, err := sc.SyscallConn()
		if err != nil {
			continue // closed by the driver
		}
		var (
			n      int
			sysErr error
		)
		ctrlErr := raw.Control(func(fd uintptr) {
			var buf [4096]byte
			n, _, sysErr = syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
		})
		if ctrlErr != nil {
			continue // closed by the driver
		}
		if sysErr == syscall.EAGAIN || sysErr == syscall.EWOULDBLOCK {
			continue // empty receive buffer: what connCheck expects of a healthy idle conn
		}
		if sysErr != nil {
			t.Logf("conn[%d]: peek error: %v", i, sysErr)
			continue
		}
		if n > 0 {
			pending[i] = n
		}
	}
	return pending
}

type churnReport struct {
	dials            int
	pendingSightings int
	maxPending       int
}

// runChurnScenario opens a fresh pool with roomy limits, runs op sequentially
// `iterations` times, and after every iteration peeks all idle sockets.
//
// With useTLS the driver connects to the secure port. connCheck() peeks the
// raw TCP socket beneath tls.Conn, so any TLS-layer record arriving while the
// connection idles (session tickets, key updates, alerts) would be flagged as
// unexpected bytes; this variant detects those too.
func runChurnScenario(t *testing.T, settings clickhouse.Settings, useTLS bool, iterations int, op func(t *testing.T, conn clickhouse.Conn, i int)) churnReport {
	t.Helper()

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	tracker := &connTracker{}
	opts := ClientOptionsFromEnv(env, settings, false)
	opts.MaxOpenConns = 5
	opts.MaxIdleConns = 5
	opts.ConnMaxLifetime = time.Hour
	if useTLS {
		opts.Addr = []string{fmt.Sprintf("%s:%d", env.Host, env.SslPort)}
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		opts.TLS = tlsConfig
		// Options.DialContext bypasses the driver's own TLS wrapping, so wrap
		// here: count the physical dial, track the raw conn for peeking, and
		// hand the tls.Conn to the driver.
		opts.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
			raw, err := tracker.DialContext(ctx, addr)
			if err != nil {
				return nil, err
			}
			tlsConn := tls.Client(raw, tlsConfig)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				raw.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	} else {
		opts.DialContext = tracker.DialContext
	}

	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	report := churnReport{}
	observePending := func(stage string) {
		if pending := tracker.pendingByConn(t); len(pending) > 0 {
			report.pendingSightings += len(pending)
			for id, n := range pending {
				if n > report.maxPending {
					report.maxPending = n
				}
				t.Logf("REPRODUCED: %s: idle conn[%d] has %d unread bytes (connCheck would kill it)", stage, id, n)
			}
		}
	}

	for i := 0; i < iterations; i++ {
		tracker.iter.Store(int64(i))
		op(t, conn, i)
		observePending(fmt.Sprintf("iteration %d", i))
	}

	// Late-arriving bytes: give the server a generous window to emit anything
	// asynchronous (profile events, logs) after the last query completed.
	for _, delay := range []time.Duration{50 * time.Millisecond, 250 * time.Millisecond} {
		time.Sleep(delay)
		observePending(fmt.Sprintf("idle after %s", delay))
	}

	// The pooled connection must still be usable without a re-dial.
	dialsBefore := tracker.Dials()
	var one uint8
	require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&one))
	if tracker.Dials() > dialsBefore {
		t.Logf("REPRODUCED: final query after idle period forced dial #%d", tracker.Dials())
	}

	report.dials = tracker.Dials()
	stats := conn.Stats()
	t.Logf("dials=%d (at iterations %v) iterations=%d pendingSightings=%d maxPending=%d stats=%+v",
		report.dials, tracker.DialIterations(), iterations, report.pendingSightings, report.maxPending, stats)
	return report
}

// requireNoChurn asserts a sequential workload reused a single physical
// connection. query() releases the connection back to the pool before
// rows.Close() returns, so even a tight loop must find the previous
// connection in the pool. Any extra dial is a healthy connection killed and
// re-established (TCP + handshake + auth), i.e. the reported slowdown.
func requireNoChurn(t *testing.T, report churnReport) {
	t.Helper()
	require.Zerof(t, report.pendingSightings, "unread bytes observed on idle pooled connection")
	require.Equalf(t, 1, report.dials,
		"connection churn: %d dials for a sequential workload", report.dials)
}

// TestConnCheckChurnSequential runs single-goroutine workloads that must all
// hold dials at ~1. Systematic extra dials are healthy connections killed and
// re-established (TCP + native handshake + auth), i.e. the reported slowdown.
func TestConnCheckChurnSequential(t *testing.T) {
	SkipOnCloud(t, "requires local socket instrumentation")

	t.Run("query row", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 200, func(t *testing.T, conn clickhouse.Conn, i int) {
			var v uint8
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&v))
		})
		requireNoChurn(t, report)
	})

	t.Run("query full read", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 100, func(t *testing.T, conn clickhouse.Conn, i int) {
			rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 100000")
			require.NoError(t, err)
			var n uint64
			for rows.Next() {
				require.NoError(t, rows.Scan(&n))
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
		requireNoChurn(t, report)
	})

	t.Run("query with per-op timeout ctx", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 200, func(t *testing.T, conn clickhouse.Conn, i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var v uint64
			require.NoError(t, conn.QueryRow(ctx, "SELECT sum(number) FROM numbers(1000)").Scan(&v))
		})
		requireNoChurn(t, report)
	})

	t.Run("query with server logs", func(t *testing.T) {
		report := runChurnScenario(t, clickhouse.Settings{"send_logs_level": "trace"}, false, 100, func(t *testing.T, conn clickhouse.Conn, i int) {
			rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 1000")
			require.NoError(t, err)
			for rows.Next() {
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
		requireNoChurn(t, report)
	})

	t.Run("early rows close", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 30, func(t *testing.T, conn clickhouse.Conn, i int) {
			rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 1000000")
			require.NoError(t, err)
			require.True(t, rows.Next()) // abandon the rest
			require.NoError(t, rows.Close())
		})
		requireNoChurn(t, report)
	})

	t.Run("exec", func(t *testing.T) {
		table := "test_conncheck_exec"
		report := runChurnScenario(t, nil, false, 100, func(t *testing.T, conn clickhouse.Conn, i int) {
			if i == 0 {
				require.NoError(t, conn.Exec(context.Background(),
					fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (n UInt64) ENGINE = Null", table)))
				t.Cleanup(func() {
					conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				})
			}
			require.NoError(t, conn.Exec(context.Background(),
				fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(1000)", table)))
		})
		requireNoChurn(t, report)
	})

	t.Run("batch insert", func(t *testing.T) {
		table := "test_conncheck_batch"
		report := runChurnScenario(t, nil, false, 100, func(t *testing.T, conn clickhouse.Conn, i int) {
			if i == 0 {
				require.NoError(t, conn.Exec(context.Background(),
					fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (n UInt64) ENGINE = Null", table)))
				t.Cleanup(func() {
					conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				})
			}
			batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", table))
			require.NoError(t, err)
			for j := 0; j < 1000; j++ {
				require.NoError(t, batch.Append(uint64(j)))
			}
			require.NoError(t, batch.Send())
		})
		requireNoChurn(t, report)
	})
}

// TestConnCheckChurnErrorPaths covers the error-release paths.
//
// A server exception terminates the response stream at a packet boundary and
// the server preserves the connection, so the driver keeps it (verifying with
// a ping before reuse). A context cancellation mid-query still closes the
// socket by design: the response is not drained, so the connection cannot be
// reused safely.
func TestConnCheckChurnErrorPaths(t *testing.T) {
	SkipOnCloud(t, "requires local socket instrumentation")

	t.Run("server exception keeps connection", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 50, func(t *testing.T, conn clickhouse.Conn, i int) {
			var v uint8
			err := conn.QueryRow(context.Background(), "SELECT throwIf(1)").Scan(&v)
			require.Error(t, err)
		})
		requireNoChurn(t, report)
	})

	t.Run("mid-stream server exception keeps connection", func(t *testing.T) {
		report := runChurnScenario(t, nil, false, 25, func(t *testing.T, conn clickhouse.Conn, i int) {
			// exception is raised after several data blocks were streamed
			rows, err := conn.Query(context.Background(),
				"SELECT throwIf(number = 50000), number FROM system.numbers")
			if err == nil {
				for rows.Next() {
				}
				rows.Close()
				err = rows.Err()
			}
			require.Error(t, err)
			exc := &clickhouse.Exception{}
			require.ErrorAsf(t, err, &exc, "expected a server exception, got: %v", err)
		})
		requireNoChurn(t, report)
	})

	t.Run("context cancel closes connection", func(t *testing.T) {
		const iterations = 20
		report := runChurnScenario(t, nil, false, iterations, func(t *testing.T, conn clickhouse.Conn, i int) {
			ctx, cancel := context.WithCancel(context.Background())
			rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10000000")
			if err == nil {
				rows.Next()
				cancel()
				rows.Close()
			} else {
				cancel()
			}
		})
		require.Zero(t, report.pendingSightings, "unread bytes observed on idle pooled connection")
		t.Logf("cancellation workload re-dialed %d times in %d iterations", report.dials, iterations)
		require.Greaterf(t, report.dials, iterations/2,
			"every mid-query cancellation must burn the connection (undrained socket)")
	})
}

// TestConnCheckChurnTLS repeats the key workloads over TLS. connCheck()
// unwraps tls.Conn and peeks the raw TCP socket, so a TLS record arriving
// while the connection idles in the pool (e.g. a late session ticket or key
// update from the server's OpenSSL stack) would be indistinguishable from
// protocol desync and would kill the connection.
func TestConnCheckChurnTLS(t *testing.T) {
	SkipOnCloud(t, "requires local socket instrumentation")

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	if env.SslPort == 0 {
		t.Skip("test environment has no TLS port")
	}

	t.Run("query row", func(t *testing.T) {
		report := runChurnScenario(t, nil, true, 200, func(t *testing.T, conn clickhouse.Conn, i int) {
			var v uint8
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&v))
		})
		requireNoChurn(t, report)
	})

	t.Run("query full read", func(t *testing.T) {
		report := runChurnScenario(t, nil, true, 100, func(t *testing.T, conn clickhouse.Conn, i int) {
			rows, err := conn.Query(context.Background(), "SELECT number FROM system.numbers LIMIT 100000")
			require.NoError(t, err)
			var n uint64
			for rows.Next() {
				require.NoError(t, rows.Scan(&n))
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
		requireNoChurn(t, report)
	})

	t.Run("batch insert", func(t *testing.T) {
		table := "test_conncheck_tls_batch"
		report := runChurnScenario(t, nil, true, 50, func(t *testing.T, conn clickhouse.Conn, i int) {
			if i == 0 {
				require.NoError(t, conn.Exec(context.Background(),
					fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (n UInt64) ENGINE = Null", table)))
				t.Cleanup(func() {
					conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
				})
			}
			batch, err := conn.PrepareBatch(context.Background(), fmt.Sprintf("INSERT INTO %s", table))
			require.NoError(t, err)
			for j := 0; j < 1000; j++ {
				require.NoError(t, batch.Append(uint64(j)))
			}
			require.NoError(t, batch.Send())
		})
		requireNoChurn(t, report)
	})
}

// TestConnCheckChurnConcurrent measures dial churn under concurrency. With
// workers <= MaxOpenConns and roomy MaxIdleConns, the pool should converge on
// at most `workers` physical connections; continued dial growth would indicate
// healthy connections being discarded.
func TestConnCheckChurnConcurrent(t *testing.T) {
	SkipOnCloud(t, "requires local socket instrumentation")

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)

	tracker := &connTracker{}
	opts := ClientOptionsFromEnv(env, nil, false)
	opts.DialContext = tracker.DialContext
	opts.MaxOpenConns = 10
	opts.MaxIdleConns = 10
	opts.ConnMaxLifetime = time.Hour

	conn, err := clickhouse.Open(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	const (
		workers    = 8
		iterations = 50
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				var v uint64
				require.NoError(t, conn.QueryRow(context.Background(), "SELECT sum(number) FROM numbers(10000)").Scan(&v))
			}
		}()
	}
	wg.Wait()

	pending := tracker.pendingByConn(t)
	t.Logf("dials=%d workers=%d iterations=%d pending=%v stats=%+v",
		tracker.Dials(), workers, iterations, pending, conn.Stats())
	require.Empty(t, pending, "unread bytes observed on idle pooled connection")
	require.LessOrEqualf(t, tracker.Dials(), workers*2,
		"connection churn: %d dials for %d workers", tracker.Dials(), workers)
}

// blackholeProxy forwards TCP traffic to a target ClickHouse server.
// silenceExisting simulates a stateful middlebox (NAT, load balancer)
// silently dropping established idle flows: existing tunnels stop forwarding
// in both directions but the client-side socket stays open and never receives
// a FIN, leaving the client with a half-open connection. New connections
// tunnel normally.
type blackholeProxy struct {
	ln net.Listener

	mu      sync.Mutex
	tunnels []*proxyTunnel
	closed  bool
}

type proxyTunnel struct {
	client, server net.Conn
	silenced       atomic.Bool
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &blackholeProxy{ln: ln}
	t.Cleanup(p.close)

	// accept loop; exits when the listener is closed via t.Cleanup
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			tn := &proxyTunnel{client: client, server: server}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				client.Close()
				server.Close()
				return
			}
			p.tunnels = append(p.tunnels, tn)
			p.mu.Unlock()
			// each copy goroutine exits when either side of its tunnel closes
			go tn.pump(server, client)
			go tn.pump(client, server)
		}
	}()

	return p
}

func (p *blackholeProxy) addr() string {
	return p.ln.Addr().String()
}

// silenceExisting makes every currently established tunnel go dark without
// closing the client side.
func (p *blackholeProxy) silenceExisting() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, tn := range p.tunnels {
		tn.silenced.Store(true)
		tn.server.Close()
	}
}

func (p *blackholeProxy) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	p.ln.Close()
	for _, tn := range p.tunnels {
		tn.client.Close()
		tn.server.Close()
	}
}

func (tn *proxyTunnel) pump(dst, src net.Conn) {
	io.Copy(dst, src)
	// propagate teardown to the client only for live tunnels: a silenced
	// tunnel must leave the client half-open (no FIN)
	if !tn.silenced.Load() {
		tn.client.Close()
	}
	tn.server.Close()
}

// halfOpenOptions builds a pool routed through a blackhole proxy with a short
// idle-ping threshold.
func halfOpenOptions(t *testing.T, tracker *connTracker, pingThreshold time.Duration) (clickhouse.Options, *blackholeProxy) {
	t.Helper()

	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	proxy := startBlackholeProxy(t, fmt.Sprintf("%s:%d", env.Host, env.Port))

	opts := ClientOptionsFromEnv(env, nil, false)
	opts.Addr = []string{proxy.addr()}
	opts.DialContext = tracker.DialContext
	opts.MaxOpenConns = 2
	opts.MaxIdleConns = 2
	opts.ConnMaxLifetime = time.Hour
	opts.DialTimeout = 2 * time.Second
	opts.ConnIdlePingThreshold = pingThreshold

	return opts, proxy
}

// TestConnCheckHalfOpenConnection verifies that a pooled connection whose
// network path silently died is detected before reuse. No FIN ever reaches
// the client, so connCheck's socket peek sees a healthy, empty socket; only
// the idle revalidation ping (Options.ConnIdlePingThreshold) can catch it.
//
// Both idle regimes must be covered: idle shorter than the internal 1s
// liveness window (the socket peek is skipped entirely) and idle beyond it
// (the peek runs, passes, and must NOT suppress the ping — a passing peek is
// not proof of liveness).
func TestConnCheckHalfOpenConnection(t *testing.T) {
	SkipOnCloud(t, "requires a local TCP proxy")

	scenarios := []struct {
		name      string
		threshold time.Duration
		idle      time.Duration
	}{
		{"idle within liveness window", 200 * time.Millisecond, 250 * time.Millisecond},
		{"idle beyond liveness window", 1200 * time.Millisecond, 1500 * time.Millisecond},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			tracker := &connTracker{}
			opts, proxy := halfOpenOptions(t, tracker, sc.threshold)

			conn, err := clickhouse.Open(&opts)
			require.NoError(t, err)
			t.Cleanup(func() { conn.Close() })

			var v uint8
			require.NoError(t, conn.QueryRow(context.Background(), "SELECT 1").Scan(&v))
			require.Equal(t, 1, tracker.Dials())

			proxy.silenceExisting()
			time.Sleep(sc.idle)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			start := time.Now()
			require.NoError(t, conn.QueryRow(ctx, "SELECT 1").Scan(&v),
				"query must succeed on a fresh connection after the half-open one is discarded")
			require.Equalf(t, 2, tracker.Dials(),
				"revalidation must discard the half-open connection and dial exactly one replacement")
			require.Lessf(t, time.Since(start), 2*opts.DialTimeout,
				"revalidation must fail fast, not hang until read timeout")
		})
	}

	t.Run("database sql", func(t *testing.T) {
		tracker := &connTracker{}
		opts, proxy := halfOpenOptions(t, tracker, 1200*time.Millisecond)

		db := clickhouse.OpenDB(&opts)
		t.Cleanup(func() { db.Close() })
		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(time.Hour)

		var v uint8
		require.NoError(t, db.QueryRow("SELECT 1").Scan(&v))
		require.Equal(t, 1, tracker.Dials())

		proxy.silenceExisting()
		time.Sleep(1500 * time.Millisecond)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&v),
			"ResetSession must discard the half-open connection so database/sql retries on a fresh one")
		require.Equal(t, 2, tracker.Dials())
	})
}
