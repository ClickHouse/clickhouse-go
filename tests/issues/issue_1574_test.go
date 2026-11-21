package issues

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// faultConn1574 wraps net.Conn to inject deterministic pre-write failures or
// post-write read drops while keeping control over the underlying TCP socket.
type faultConn1574 struct {
	net.Conn
	raw net.Conn

	armReadDrop       atomic.Bool
	firstReadConsumed atomic.Bool
	armWriteFail      atomic.Bool

	trackingWrites atomic.Bool
	wroteBytes     atomic.Bool
	writeCalls     atomic.Int32
}

func (c *faultConn1574) Write(p []byte) (int, error) {
	if c.armWriteFail.CompareAndSwap(true, false) {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
	}
	if c.trackingWrites.Load() && len(p) > 0 {
		c.wroteBytes.Store(true)
		c.writeCalls.Add(1)
	}
	return c.Conn.Write(p)
}

func (c *faultConn1574) dropErr() error {
	return &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
}

func (c *faultConn1574) Read(p []byte) (int, error) {
	if c.armReadDrop.Load() && (!c.trackingWrites.Load() || c.wroteBytes.Load()) {
		if c.firstReadConsumed.CompareAndSwap(false, true) {
			// Wait for the server to finish and begin replying before dropping
			// the read side, so server-side execution (e.g. INSERT) completes.
			_, _ = c.Conn.Read(p)
		}
		return 0, c.dropErr()
	}
	n, err := c.Conn.Read(p)
	// Also intercept a Peek/Read that entered c.Conn.Read before armReadDrop was set
	// (e.g. net/http's idle persistConn.readLoop waiting on Peek(1) across queries).
	if c.armReadDrop.Load() && (!c.trackingWrites.Load() || c.wroteBytes.Load()) {
		c.firstReadConsumed.Store(true)
		return 0, c.dropErr()
	}
	return n, err
}

func (c *faultConn1574) startBreakOnRead() {
	c.wroteBytes.Store(false)
	c.writeCalls.Store(0)
	c.firstReadConsumed.Store(false)
	c.trackingWrites.Store(true)
	c.armReadDrop.Store(true)
}

func (c *faultConn1574) startMidStreamDrop() {
	c.trackingWrites.Store(false)
	c.firstReadConsumed.Store(true)
	c.armReadDrop.Store(true)
}

func issue1574OpenTestDB(
	t *testing.T,
	env clickhouse_tests.ClickHouseTestEnvironment,
	protocol clickhouse.Protocol,
	onDial func(dialNum int32, conn *faultConn1574),
) *sql.DB {
	t.Helper()
	return issue1574OpenTestDBWithDialErr(t, env, protocol, onDial, nil)
}

func issue1574OpenTestDBWithDialErr(
	t *testing.T,
	env clickhouse_tests.ClickHouseTestEnvironment,
	protocol clickhouse.Protocol,
	onDial func(dialNum int32, conn *faultConn1574),
	beforeDial func(dialNum int32) error,
) *sql.DB {
	t.Helper()

	opts := clickhouse_tests.ClientOptionsFromEnv(
		env,
		clickhouse_tests.TestClientDefaultSettings(env),
		protocol == clickhouse.HTTP,
	)

	var dials atomic.Int32
	opts.DialContext = func(ctx context.Context, addr string) (net.Conn, error) {
		dialNum := dials.Add(1)
		if beforeDial != nil {
			if err := beforeDial(dialNum); err != nil {
				return nil, err
			}
		}
		raw, err := (&net.Dialer{Timeout: opts.DialTimeout}).DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}

		// DialContext replaces opts.TLS on Native; wrap after TLS handshake so
		// handshake packets aren't intercepted by Read/Write fault flags.
		netConn := net.Conn(raw)
		if opts.Protocol == clickhouse.Native && opts.TLS != nil {
			cfg := opts.TLS.Clone()
			if cfg.ServerName == "" {
				host, _, splitErr := net.SplitHostPort(addr)
				if splitErr != nil {
					host = addr
				}
				cfg.ServerName = host
			}
			tlsConn := tls.Client(raw, cfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			netConn = tlsConn
		}

		fc := &faultConn1574{Conn: netConn, raw: raw}
		if onDial != nil {
			onDial(dialNum, fc)
		}
		return fc, nil
	}

	db := sql.OpenDB(clickhouse.Connector(&opts))
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func issue1574OpenAdminDB(
	t *testing.T,
	env clickhouse_tests.ClickHouseTestEnvironment,
	protocol clickhouse.Protocol,
) *sql.DB {
	t.Helper()

	opts := clickhouse_tests.ClientOptionsFromEnv(
		env,
		clickhouse_tests.TestClientDefaultSettings(env),
		protocol == clickhouse.HTTP,
	)
	db := clickhouse.OpenDB(&opts)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func issue1574PrepareTable(t *testing.T, ctx context.Context, adminDB *sql.DB, table string) {
	t.Helper()
	_, err := adminDB.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	require.NoError(t, err)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY tuple()", table))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adminDB.ExecContext(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	})
}

func Test1574_NoRetryAfterQuerySent(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, protocol := range []clickhouse.Protocol{clickhouse.Native, clickhouse.HTTP} {
		t.Run(fmt.Sprintf("%s Protocol", protocol), func(t *testing.T) {
			adminDB := issue1574OpenAdminDB(t, env, protocol)

			t.Run("ExecContext does not retry after query sent", func(t *testing.T) {
				table := fmt.Sprintf("issue_1574_exec_%s", protocol)
				issue1574PrepareTable(t, ctx, adminDB, table)

				var (
					dialCount atomic.Int32
					conns     [2]*faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					if int(dialNum) <= len(conns) {
						conns[dialNum-1] = fc
					}
				})

				require.NoError(t, db.PingContext(ctx))
				require.Equal(t, int32(1), dialCount.Load())

				c1 := conns[0]
				c1.startBreakOnRead()

				_, execErr := db.ExecContext(ctx, fmt.Sprintf(
					"INSERT INTO %s SELECT number, toUInt8(number) FROM numbers(10)",
					table,
				))
				require.Error(t, execErr, "broken read after write must return error instead of retrying via ErrBadConn")
				assert.Equal(t, int32(1), dialCount.Load(), "database/sql must not open a second connection to retry ExecContext")

				var rows, distinct uint64
				require.NoError(t, adminDB.QueryRowContext(ctx, fmt.Sprintf(
					"SELECT count(), uniqExact(a) FROM %s", table,
				)).Scan(&rows, &distinct))
				assert.Equal(t, uint64(10), rows, "INSERT must execute only once")
				assert.Equal(t, uint64(10), distinct)

				// Broken connection must have been evicted; next query must get a healthy connection.
				var one uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, uint64(1), one)
				assert.Equal(t, int32(2), dialCount.Load(), "broken connection must leave the pool")
			})

			t.Run("ExecContext async insert does not retry after query sent", func(t *testing.T) {
				table := fmt.Sprintf("issue_1574_async_%s", protocol)
				issue1574PrepareTable(t, ctx, adminDB, table)

				var (
					dialCount atomic.Int32
					c1        *faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					if dialNum == 1 {
						c1 = fc
					}
				})

				require.NoError(t, db.PingContext(ctx))
				require.Equal(t, int32(1), dialCount.Load())

				c1.startBreakOnRead()

				asyncCtx := clickhouse.Context(ctx, clickhouse.WithStdAsync(true))
				_, execErr := db.ExecContext(asyncCtx, fmt.Sprintf(
					"INSERT INTO %s (a, b) VALUES (1, 1), (2, 2), (3, 3)",
					table,
				))
				require.Error(t, execErr, "async insert must return error instead of retrying after send")
				assert.Equal(t, int32(1), dialCount.Load(), "database/sql must not retry async insert")

				var rows uint64
				require.NoError(t, adminDB.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", table)).Scan(&rows))
				assert.Equal(t, uint64(3), rows, "async insert must execute only once")

				var one uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, uint64(1), one)
				assert.Equal(t, int32(2), dialCount.Load())
			})

			t.Run("QueryContext does not retry after query sent", func(t *testing.T) {
				var (
					dialCount atomic.Int32
					c1        *faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					if dialNum == 1 {
						c1 = fc
					}
				})

				require.NoError(t, db.PingContext(ctx))
				require.Equal(t, int32(1), dialCount.Load())

				c1.startBreakOnRead()

				rows, queryErr := db.QueryContext(ctx, "SELECT number FROM numbers(10)")
				if rows != nil {
					_ = rows.Close()
				}
				require.Error(t, queryErr, "broken read after write must return error instead of retrying via ErrBadConn")
				assert.Equal(t, int32(1), dialCount.Load(), "database/sql must not open a second connection to retry QueryContext")

				var one uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, uint64(1), one)
				assert.Equal(t, int32(2), dialCount.Load(), "broken connection must leave the pool")
			})

			t.Run("QueryContext mid-stream error evicts broken connection", func(t *testing.T) {
				var (
					dialCount atomic.Int32
					c1        *faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					if dialNum == 1 {
						c1 = fc
					}
				})

				require.NoError(t, db.PingContext(ctx))

				rows, err := db.QueryContext(ctx, "SELECT number FROM numbers(50000) SETTINGS max_block_size = 1000")
				require.NoError(t, err)
				require.True(t, rows.Next())
				var first uint64
				require.NoError(t, rows.Scan(&first))

				// Drop connection while streaming remaining blocks.
				c1.startMidStreamDrop()
				for rows.Next() {
				}
				streamErr := rows.Err()
				closeErr := rows.Close()
				assert.True(t, streamErr != nil || closeErr != nil, "mid-stream drop must surface an error")

				// Ensure c1 itself never receives a subsequent query (it must be evicted at rows.Close/PutConn).
				c1.writeCalls.Store(0)
				c1.trackingWrites.Store(true)

				// Next query must not reuse broken c1.
				var one uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, uint64(1), one)
				assert.Equal(t, int32(0), c1.writeCalls.Load(), "broken connection c1 must leave the pool before the next query")
				assert.Equal(t, int32(2), dialCount.Load(), "connection broken mid-stream must leave the pool")
			})

			t.Run("PrepareContext does not retry after query sent", func(t *testing.T) {
				table := fmt.Sprintf("issue_1574_prepare_%s", protocol)
				issue1574PrepareTable(t, ctx, adminDB, table)

				var (
					dialCount atomic.Int32
					c1        *faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					if dialNum == 1 {
						c1 = fc
					}
				})

				require.NoError(t, db.PingContext(ctx))
				require.Equal(t, int32(1), dialCount.Load())

				c1.startBreakOnRead()

				stmt, prepErr := db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (a, b) VALUES (?, ?)", table))
				if stmt != nil {
					_ = stmt.Close()
				}
				require.Error(t, prepErr, "broken read after send must return error instead of retrying via ErrBadConn")
				assert.Equal(t, int32(1), dialCount.Load(), "database/sql must not open a second connection to retry PrepareContext")

				var one uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&one))
				assert.Equal(t, uint64(1), one)
				assert.Equal(t, int32(2), dialCount.Load(), "broken connection must leave the pool")
			})

			t.Run("Dead idle connection before query still retries", func(t *testing.T) {
				table := fmt.Sprintf("issue_1574_dead_idle_%s", protocol)
				issue1574PrepareTable(t, ctx, adminDB, table)

				var (
					dialCount atomic.Int32
					latest    *faultConn1574
				)
				db := issue1574OpenTestDB(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
					dialCount.Store(dialNum)
					latest = fc
				})

				// Warm up conn 1.
				require.NoError(t, db.PingContext(ctx))
				require.Equal(t, int32(1), dialCount.Load())

				// Close idle TCP socket before ExecContext -> must recover via retry.
				_ = latest.raw.Close()
				_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (1, 1)", table))
				require.NoError(t, err)
				prevDials := dialCount.Load()
				assert.Equal(t, int32(2), prevDials)

				// Close idle TCP socket before QueryContext -> must recover via retry.
				_ = latest.raw.Close()
				var n uint64
				require.NoError(t, db.QueryRowContext(ctx, "SELECT 1").Scan(&n))
				assert.Equal(t, uint64(1), n)
				assert.Equal(t, int32(3), dialCount.Load())

				// Close idle TCP socket before PrepareContext -> must recover via retry.
				_ = latest.raw.Close()
				stmt, err := db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (a, b) VALUES (?, ?)", table))
				require.NoError(t, err)
				require.NoError(t, stmt.Close())
				assert.Equal(t, int32(4), dialCount.Load())
			})

			t.Run("Pre-write failure before any bytes sent still retries", func(t *testing.T) {
				table := fmt.Sprintf("issue_1574_prewrite_%s", protocol)
				issue1574PrepareTable(t, ctx, adminDB, table)

				runPreWriteCase := func(t *testing.T, op func(db *sql.DB) error) {
					t.Helper()
					var (
						dialCount    atomic.Int32
						failNextDial atomic.Bool
						latest       *faultConn1574
					)
					db := issue1574OpenTestDBWithDialErr(t, env, protocol, func(dialNum int32, fc *faultConn1574) {
						dialCount.Store(dialNum)
						latest = fc
					}, func(dialNum int32) error {
						dialCount.Store(dialNum)
						if failNextDial.CompareAndSwap(true, false) {
							return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
						}
						return nil
					})

					require.NoError(t, db.PingContext(ctx))
					require.Equal(t, int32(1), dialCount.Load())

					if protocol == clickhouse.Native {
						// Fail first Write of next query on idle pooled conn (0 bytes sent).
						latest.armWriteFail.Store(true)
					} else {
						// Close the idle HTTP TCP connection so http.Transport must dial (dial 2),
						// and fail that dial before connection establishment so stdDriver sees a
						// dial error and returns driver.ErrBadConn to retry on dial 3.
						_ = latest.raw.Close()
						failNextDial.Store(true)
					}

					require.NoError(t, op(db))
					if protocol == clickhouse.Native {
						assert.Equal(t, int32(2), dialCount.Load())
					} else {
						assert.Equal(t, int32(3), dialCount.Load())
					}
				}

				t.Run("ExecContext", func(t *testing.T) {
					runPreWriteCase(t, func(db *sql.DB) error {
						_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (42, 1)", table))
						return err
					})
				})

				t.Run("QueryContext", func(t *testing.T) {
					runPreWriteCase(t, func(db *sql.DB) error {
						var v uint64
						return db.QueryRowContext(ctx, "SELECT 1").Scan(&v)
					})
				})

				t.Run("PrepareContext", func(t *testing.T) {
					runPreWriteCase(t, func(db *sql.DB) error {
						stmt, err := db.PrepareContext(ctx, fmt.Sprintf("INSERT INTO %s (a, b) VALUES (?, ?)", table))
						if err != nil {
							return err
						}
						return stmt.Close()
					})
				})
			})
		})
	}
}
