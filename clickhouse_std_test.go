package clickhouse

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type scriptedNetConn struct {
	net.Conn
	writeFn func([]byte) (int, error)
	readFn  func([]byte) (int, error)
}

func (c *scriptedNetConn) Write(b []byte) (int, error) {
	if c.writeFn != nil {
		return c.writeFn(b)
	}
	return c.Conn.Write(b)
}

func (c *scriptedNetConn) Read(b []byte) (int, error) {
	if c.readFn != nil {
		return c.readFn(b)
	}
	return c.Conn.Read(b)
}

func newStdTestConnect(nc net.Conn) *connect {
	c := createMockConnect(&mockNetConn{})
	c.conn = nc
	c.reader = chproto.NewReader(nc)
	c.connectedAt = time.Now()
	c.opt = &Options{ConnMaxLifetime: time.Hour}
	c.compression = CompressionNone
	return c
}

func TestStdDriver_ValidatorBasics(t *testing.T) {
	prototype := &stdDriver{logger: newNoopLogger()}
	assert.False(t, prototype.IsValid(), "prototype driver with nil conn must not be reported valid")

	healthy := &stdDriver{conn: newStdTestConnect(&mockNetConn{}), logger: newNoopLogger()}
	assert.True(t, healthy.IsValid())
	assert.NoError(t, healthy.ResetSession(context.Background()))

	healthy.markBroken()
	assert.False(t, healthy.IsValid())
	assert.ErrorIs(t, healthy.ResetSession(context.Background()), driver.ErrBadConn)

	// IsValid must check only broken/conn without calling healthCheck on return to pool;
	// ResetSession performs healthCheck before the next borrow.
	expired := newStdTestConnect(&mockNetConn{})
	expired.connectedAt = time.Now().Add(-2 * time.Hour)
	expiredStd := &stdDriver{conn: expired, logger: newNoopLogger()}
	assert.True(t, expiredStd.IsValid())
	assert.ErrorIs(t, expiredStd.ResetSession(context.Background()), driver.ErrBadConn)
	assert.False(t, expiredStd.IsValid())
}

func TestStdDriver_NativeWriteBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("first write failure returns ErrBadConn", func(t *testing.T) {
		writeErr := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
		conn := newStdTestConnect(&mockNetConn{writeErr: writeErr})
		std := &stdDriver{conn: conn, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "SELECT 1", nil)
		require.ErrorIs(t, err, driver.ErrBadConn)
		assert.False(t, std.IsValid())
		assert.ErrorIs(t, std.ResetSession(ctx), driver.ErrBadConn)
	})

	t.Run("partial write returns original error and evicts conn", func(t *testing.T) {
		writeErr := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
		sc := &scriptedNetConn{
			Conn: &mockNetConn{},
			writeFn: func([]byte) (int, error) {
				return 8, writeErr
			},
		}
		std := &stdDriver{conn: newStdTestConnect(sc), logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
		require.Error(t, err)
		assert.NotErrorIs(t, err, driver.ErrBadConn)
		assert.ErrorIs(t, err, writeErr)
		assert.False(t, std.IsValid())
	})

	for _, tc := range []struct {
		name string
		call func(std *stdDriver) error
	}{
		{
			name: "ExecContext",
			call: func(std *stdDriver) error {
				_, err := std.ExecContext(ctx, "INSERT INTO t SELECT 1", nil)
				return err
			},
		},
		{
			name: "ExecContext async insert",
			call: func(std *stdDriver) error {
				asyncCtx := Context(ctx, WithStdAsync(true))
				_, err := std.ExecContext(asyncCtx, "INSERT INTO t VALUES (1)", nil)
				return err
			},
		},
		{
			name: "QueryContext",
			call: func(std *stdDriver) error {
				rows, err := std.QueryContext(ctx, "SELECT 1", nil)
				if rows != nil {
					_ = rows.Close()
				}
				return err
			},
		},
		{
			name: "PrepareContext",
			call: func(std *stdDriver) error {
				stmt, err := std.PrepareContext(ctx, "INSERT INTO t (a) VALUES (?)")
				if stmt != nil {
					_ = stmt.Close()
				}
				return err
			},
		},
	} {
		t.Run(tc.name+" read drop after send returns original error", func(t *testing.T) {
			readErr := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
			sc := &scriptedNetConn{
				Conn: &mockNetConn{},
				readFn: func([]byte) (int, error) {
					return 0, readErr
				},
			}
			std := &stdDriver{conn: newStdTestConnect(sc), logger: newNoopLogger()}

			err := tc.call(std)
			require.Error(t, err)
			assert.NotErrorIs(t, err, driver.ErrBadConn)
			assert.ErrorIs(t, err, readErr)
			assert.False(t, std.IsValid())
			assert.ErrorIs(t, std.ResetSession(ctx), driver.ErrBadConn)
		})
	}

	t.Run("server exception keeps connection valid", func(t *testing.T) {
		var buf chproto.Buffer
		buf.PutByte(proto.ServerException)
		buf.PutInt32(60)
		buf.PutString("DB::Exception")
		buf.PutString("Table default.missing doesn't exist")
		buf.PutString("")
		buf.PutBool(false)
		payload := append([]byte(nil), buf.Buf...)

		r := bytes.NewReader(payload)
		sc := &scriptedNetConn{
			Conn:   &mockNetConn{},
			readFn: r.Read,
		}
		std := &stdDriver{conn: newStdTestConnect(sc), logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "SELECT * FROM missing", nil)
		require.Error(t, err)
		var ex *Exception
		require.ErrorAs(t, err, &ex)
		assert.Equal(t, int32(60), ex.Code)
		assert.True(t, std.IsValid(), "server SQL exception must not evict a healthy connection")
	})

	t.Run("canceled context never maps broken error to ErrBadConn", func(t *testing.T) {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		writeErr := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
		std := &stdDriver{
			conn:   newStdTestConnect(&mockNetConn{writeErr: writeErr}),
			logger: newNoopLogger(),
		}

		_, err := std.ExecContext(cancelCtx, "SELECT 1", nil)
		require.Error(t, err)
		assert.NotErrorIs(t, err, driver.ErrBadConn)
	})

	t.Run("stdRows stream error marks connection invalid", func(t *testing.T) {
		std := &stdDriver{
			conn:   newStdTestConnect(&mockNetConn{}),
			logger: newNoopLogger(),
		}
		errCh := make(chan error, 1)
		errCh <- io.EOF
		close(errCh)
		stream := make(chan *proto.Block)
		close(stream)

		sr := &stdRows{
			rows: &rows{
				block:   proto.NewBlock(),
				errors:  errCh,
				stream:  stream,
				columns: []string{},
			},
			logger:      std.logger,
			onStreamErr: std.markBrokenIfFatal,
		}

		err := sr.Next([]driver.Value{})
		require.ErrorIs(t, err, io.EOF)
		assert.False(t, std.IsValid())
	})

	t.Run("PrepareContext invalid query error does not mark connection invalid", func(t *testing.T) {
		std := &stdDriver{conn: newStdTestConnect(&mockNetConn{}), logger: newNoopLogger()}
		_, err := std.PrepareContext(ctx, "SELECT 1")
		require.Error(t, err)
		assert.NotErrorIs(t, err, driver.ErrBadConn)
		assert.True(t, std.IsValid())
	})

	t.Run("ErrBadConn returned after send is sanitized and marks connection invalid", func(t *testing.T) {
		conn := newStdTestConnect(&mockNetConn{})
		conn.wroteBytes.Store(true)
		std := &stdDriver{conn: conn, logger: newNoopLogger()}

		outErr := std.handleOpError(ctx, "exec context", fmt.Errorf("wrapped: %w", driver.ErrBadConn))
		require.Error(t, outErr)
		assert.NotErrorIs(t, outErr, driver.ErrBadConn)
		assert.Contains(t, outErr.Error(), "bad connection")
		assert.False(t, std.IsValid())
	})
}

func newStdTestHTTPConnect(t *testing.T, opt *Options) *httpConnect {
	t.Helper()
	rt, err := createHTTPRoundTripper(opt)
	require.NoError(t, err)

	compPool, err := createCompressionPool(&Compression{Method: CompressionNone})
	require.NoError(t, err)

	u, err := url.Parse("http://" + opt.Addr[0] + "/?default_format=Native")
	require.NoError(t, err)

	return &httpConnect{
		id:              1,
		connectedAt:     time.Now(),
		logger:          newNoopLogger(),
		opt:             opt,
		url:             u,
		client:          &http.Client{Transport: rt},
		compression:     CompressionNone,
		compressionPool: compPool,
		handshake:       proto.ServerHandshake{Timezone: time.UTC},
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestStdDriver_HTTPWriteBoundary(t *testing.T) {
	ctx := context.Background()

	t.Run("dial error before send returns ErrBadConn", func(t *testing.T) {
		dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{"127.0.0.1:1"},
			DialContext: func(context.Context, string) (net.Conn, error) {
				return nil, dialErr
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "SELECT 1", nil)
		require.ErrorIs(t, err, driver.ErrBadConn)
		assert.False(t, std.IsValid())
	})

	t.Run("proxyconnect error before send returns ErrBadConn", func(t *testing.T) {
		proxyErr := &net.OpError{Op: "proxyconnect", Net: "tcp", Err: syscall.ECONNREFUSED}
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{"127.0.0.1:8123"},
			TransportFunc: func(*http.Transport) (http.RoundTripper, error) {
				return roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, proxyErr
				}), nil
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "SELECT 1", nil)
		require.ErrorIs(t, err, driver.ErrBadConn)
		assert.False(t, std.IsValid())
	})

	t.Run("write error on non-reused dial returns original error conservatively", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		writeErr := &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}
		addr := strings.TrimPrefix(srv.URL, "http://")
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{addr},
			DialContext: func(ctx context.Context, a string) (net.Conn, error) {
				raw, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", a)
				if err != nil {
					return nil, err
				}
				return &scriptedNetConn{
					Conn: raw,
					writeFn: func([]byte) (int, error) {
						return 0, writeErr
					},
				}, nil
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
		require.Error(t, err)
		assert.NotErrorIs(t, err, driver.ErrBadConn)
		assert.ErrorIs(t, err, writeErr)
		assert.False(t, std.IsValid())
	})

	t.Run("read reset after request written returns original error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		addr := strings.TrimPrefix(srv.URL, "http://")
		readErr := &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}
		var wrote atomic.Bool
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{addr},
			DialContext: func(ctx context.Context, a string) (net.Conn, error) {
				raw, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", a)
				if err != nil {
					return nil, err
				}
				return &scriptedNetConn{
					Conn: raw,
					writeFn: func(p []byte) (int, error) {
						if len(p) > 0 {
							wrote.Store(true)
						}
						return raw.Write(p)
					},
					readFn: func(p []byte) (int, error) {
						if wrote.Load() {
							_, _ = raw.Read(p)
							return 0, readErr
						}
						n, err := raw.Read(p)
						if wrote.Load() {
							return 0, readErr
						}
						return n, err
					},
				}, nil
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "INSERT INTO t SELECT 1", nil)
		require.Error(t, err)
		assert.True(t, wrote.Load())
		assert.NotErrorIs(t, err, driver.ErrBadConn)
		assert.ErrorIs(t, err, readErr)
		assert.False(t, std.IsValid())
	})

	t.Run("two requests in one operation where second fails on dial avoids retry", func(t *testing.T) {
		var calls atomic.Int32
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{"127.0.0.1:8123"},
			TransportFunc: func(*http.Transport) (http.RoundTripper, error) {
				return roundTripFunc(func(*http.Request) (*http.Response, error) {
					if calls.Add(1) == 1 {
						return &http.Response{
							StatusCode: http.StatusOK,
							Body:       io.NopCloser(strings.NewReader("")),
						}, nil
					}
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
				}), nil
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}
		h.resetWriteState()

		req1, err := h.prepareRequest(ctx, "DESCRIBE TABLE t", &QueryOptions{}, nil)
		require.NoError(t, err)
		resp1, err := h.executeRequest(req1)
		require.NoError(t, err)
		require.NoError(t, resp1.Body.Close())

		req2, err := h.prepareRequest(ctx, "INSERT INTO t VALUES (1)", &QueryOptions{}, nil)
		require.NoError(t, err)
		_, dialErr := h.executeRequest(req2)
		require.Error(t, dialErr)

		assert.True(t, h.hasWrittenBytes(), "sticky wroteBytes must stay true across the operation")
		outErr := std.handleOpError(ctx, "exec context", dialErr)
		require.Error(t, outErr)
		assert.NotErrorIs(t, outErr, driver.ErrBadConn)
		var opErr *net.OpError
		require.ErrorAs(t, outErr, &opErr)
		assert.Equal(t, "dial", opErr.Op)
		assert.False(t, std.IsValid())
	})

	t.Run("custom TransportFunc returning EOF avoids retry", func(t *testing.T) {
		h := newStdTestHTTPConnect(t, &Options{
			Addr: []string{"127.0.0.1:8123"},
			TransportFunc: func(*http.Transport) (http.RoundTripper, error) {
				return roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, io.EOF
				}), nil
			},
		})
		std := &stdDriver{conn: h, logger: newNoopLogger()}

		_, err := std.ExecContext(ctx, "INSERT INTO t VALUES (1)", nil)
		require.Error(t, err)
		assert.NotErrorIs(t, err, driver.ErrBadConn)
		assert.ErrorIs(t, err, io.EOF)
		assert.False(t, std.IsValid())
	})
}
