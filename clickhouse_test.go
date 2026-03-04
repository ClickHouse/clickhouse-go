//go:build go1.25

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// TestConnectionPool_Open demonstrates that drainPool goroutines
// are not leaked when connections are opened and closed.
func TestConnectionPool_Open(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn, err := Open(&Options{
			Addr: []string{"localhost:9000"},
		})
		if err != nil {
			t.Fatalf("failed to open connection: %v", err)
		}

		// Close the connection - this should stop the goroutine
		if err := conn.Close(); err != nil {
			t.Fatalf("failed to close connection: %v", err)
		}

		// Wait for all goroutines in this synctest bubble to exit
		// This will panic if background goroutines are left behind.
		synctest.Wait()
	})
}

// TestConnectionPool_OpenConcurrent demonstrates that drainPool goroutines
// are not leaked when connections are opened and closed.
func TestConnectionPool_OpenConcurrent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errs := make(chan error)

		var wg sync.WaitGroup
		for range 100 {
			wg.Go(func() {
				conn, err := Open(&Options{
					Addr: []string{"localhost:9000"},
				})
				if err != nil {
					errs <- fmt.Errorf("failed to open connection: %w", err)
				}

				// Close the connection - this should stop the goroutine
				if err := conn.Close(); err != nil {
					errs <- fmt.Errorf("failed to close connection: %w", err)
				}
			})
		}

		go func() {
			wg.Wait()
			close(errs)
		}()

		for err := range errs {
			// any error attempting to open or close should be fatal
			t.Fatal(err)
		}

		// Wait for all goroutines in this synctest bubble to exit
		// This will panic if background goroutines are left behind.
		synctest.Wait()
	})
}

// TestAcquire_NewConnection tests acquiring a new connection when pool is empty
func TestAcquire_NewConnection(t *testing.T) {
	dialCount := atomic.Int32{}

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			dialCount.Add(1)
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)
	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	if transport == nil {
		t.Fatal("expected connection, got nil")
	}

	if dialCount.Load() != 1 {
		t.Errorf("expected 1 dial call, got %d", dialCount.Load())
	}

	if transport.isReleased() {
		t.Error("newly acquired connection should not be marked as released")
	}
}

// TestAcquire_ReuseIdleConnection tests reusing a healthy idle connection
func TestAcquire_ReuseIdleConnection(t *testing.T) {
	dialCount := atomic.Int32{}

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			dialCount.Add(1)
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// Acquire and release a connection
	conn1, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	ch.release(conn1, nil)

	// Acquire again - should reuse the same connection
	conn2, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}

	if dialCount.Load() != 1 {
		t.Errorf("expected 1 dial call (reused connection), got %d", dialCount.Load())
	}

	if conn1.connID() != conn2.connID() {
		t.Error("expected same connection to be reused")
	}
}

// TestAcquire_BadConnection tests acquiring when pool has a bad connection
func TestAcquire_BadConnection(t *testing.T) {
	dialCount := atomic.Int32{}
	var connID atomic.Int64

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, id int, opt *Options, dial Dial) (DialResult, error) {
			dialCount.Add(1)
			nextID := int(connID.Add(1))
			return DialResult{conn: newMockTransport(nextID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// Acquire and release a connection
	conn1, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}

	// Mark it as bad before releasing
	mock1 := conn1.(*mockTransport)
	mock1.setBad(true)
	ch.release(conn1, nil)

	// Acquire again - should detect bad connection, close it, and dial new one
	conn2, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}

	if dialCount.Load() != 2 {
		t.Errorf("expected 2 dial calls (bad connection replaced), got %d", dialCount.Load())
	}

	if conn1.connID() == conn2.connID() {
		t.Error("expected different connection after bad connection detected")
	}

	if !mock1.isClosed() {
		t.Error("bad connection should be closed")
	}
}

// TestAcquire_MaxOpenConnsLimit tests that MaxOpenConns limit is respected
func TestAcquire_MaxOpenConnsLimit(t *testing.T) {
	maxOpen := 2

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     20 * time.Millisecond,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// Acquire up to max
	conns := make([]nativeTransport, maxOpen)
	for i := 0; i < maxOpen; i++ {
		transport, err := ch.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		conns[i] = transport
	}

	// Try to acquire one more - should timeout
	_, err = ch.acquire(context.Background())
	if err == nil {
		t.Fatal("expected error when exceeding MaxOpenConns")
	}

	if !errors.Is(err, ErrAcquireConnTimeout) {
		t.Errorf("expected ErrAcquireConnTimeout, got %v", err)
	}
}

// TestAcquire_ClosedConnection tests acquiring from closed connection pool
func TestAcquire_ClosedConnection(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	ch := conn.(*clickhouse)

	// Close the connection pool
	conn.Close()

	_, err = ch.acquire(context.Background())
	if !errors.Is(err, ErrConnectionClosed) {
		t.Errorf("expected ErrConnectionClosed, got %v", err)
	}
}

// TestAcquire_DialFailure tests error handling when dial fails
func TestAcquire_DialFailure(t *testing.T) {
	expectedErr := errors.New("dial failed")

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{}, expectedErr
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	_, err = ch.acquire(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected dial error, got %v", err)
	}

	// Verify open slot was released
	if len(ch.open) != 0 {
		t.Error("open slot should be released after dial failure")
	}
}

// TestAcquire_ContextCancellation tests context cancellation during acquire
func TestAcquire_ContextCancellation(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    1,
		MaxIdleConns:    1,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// Acquire one connection to fill the pool
	conn1, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	defer ch.release(conn1, nil)

	// Try to acquire with already-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = ch.acquire(ctx)
	if err == nil {
		t.Fatal("expected error with cancelled context")
	}
}

// TestRelease_HealthyConnection tests releasing a healthy connection
func TestRelease_HealthyConnection(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	ch.release(transport, nil)

	if !transport.isReleased() {
		t.Error("connection should be marked as released")
	}

	if ch.idle.Len() != 1 {
		t.Errorf("expected 1 connection in idle pool, got %d", ch.idle.Len())
	}

	mock := transport.(*mockTransport)
	if mock.isClosed() {
		t.Error("healthy connection should not be closed")
	}
}

// TestRelease_WithError tests releasing a connection with an error
func TestRelease_WithError(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	ch.release(transport, errors.New("some error"))

	mock := transport.(*mockTransport)
	if !mock.isClosed() {
		t.Error("connection with error should be closed")
	}

	if ch.idle.Len() != 0 {
		t.Errorf("connection with error should not be returned to pool, got %d in pool", ch.idle.Len())
	}
}

// TestRelease_ExpiredConnection tests releasing an expired connection
func TestRelease_ExpiredConnection(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: 10 * time.Millisecond,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			// Create a connection with old timestamp
			mock := newMockTransport(connID)
			mock.connectedAt = time.Now().Add(-100 * time.Millisecond)
			return DialResult{conn: mock}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	ch.release(transport, nil)

	mock := transport.(*mockTransport)
	if !mock.isClosed() {
		t.Error("expired connection should be closed")
	}

	if ch.idle.Len() != 0 {
		t.Errorf("expired connection should not be returned to pool, got %d in pool", ch.idle.Len())
	}
}

// TestRelease_DoubleRelease tests that double release is idempotent
func TestRelease_DoubleRelease(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	ch.release(transport, nil)
	ch.release(transport, nil) // Second release should be no-op

	if ch.idle.Len() != 1 {
		t.Errorf("expected 1 connection in idle pool after double release, got %d", ch.idle.Len())
	}
}

// TestRelease_FreeBufOnConnRelease tests buffer freeing option
func TestRelease_FreeBufOnConnRelease(t *testing.T) {
	conn, err := Open(&Options{
		Addr:                 []string{"localhost:9000"},
		DialTimeout:          time.Second,
		MaxOpenConns:         5,
		MaxIdleConns:         2,
		ConnMaxLifetime:      time.Hour,
		FreeBufOnConnRelease: true,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	ch.release(transport, nil)

	mock := transport.(*mockTransport)
	if !mock.wasBufferFreed() {
		t.Error("buffer should be freed when FreeBufOnConnRelease is true")
	}
}

// TestRelease_WhenPoolClosed tests releasing to a closed pool
func TestRelease_WhenPoolClosed(t *testing.T) {
	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	ch := conn.(*clickhouse)

	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire failed: %v", err)
	}

	// Close the pool
	conn.Close()

	ch.release(transport, nil)

	mock := transport.(*mockTransport)
	if !mock.isClosed() {
		t.Error("connection should be closed when pool is closed")
	}
}

// TestAcquireRelease_Cycle tests acquire-release-acquire cycle
func TestAcquireRelease_Cycle(t *testing.T) {
	dialCount := atomic.Int32{}

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			dialCount.Add(1)
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// First cycle
	conn1, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("first acquire failed: %v", err)
	}
	firstID := conn1.connID()
	ch.release(conn1, nil)

	// Second cycle - should reuse
	conn2, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("second acquire failed: %v", err)
	}

	if conn2.connID() != firstID {
		t.Error("expected same connection to be reused")
	}

	if dialCount.Load() != 1 {
		t.Errorf("expected only 1 dial for reused connection, got %d", dialCount.Load())
	}
}

// TestAcquireRelease_Concurrent tests concurrent acquire and release
func TestAcquireRelease_Concurrent(t *testing.T) {
	var connID atomic.Int64

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, id int, opt *Options, dial Dial) (DialResult, error) {
			nextID := int(connID.Add(1))
			return DialResult{conn: newMockTransport(nextID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	var wg sync.WaitGroup
	numGoroutines := 50
	numIterations := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				transport, err := ch.acquire(context.Background())
				if err != nil {
					t.Errorf("acquire failed: %v", err)
					return
				}

				// Simulate some work
				time.Sleep(time.Millisecond)

				ch.release(transport, nil)
			}
		}()
	}

	wg.Wait()

	// Verify pool state
	stats := ch.Stats()
	if stats.Open > stats.MaxOpenConns {
		t.Errorf("open connections (%d) exceeded max (%d)", stats.Open, stats.MaxOpenConns)
	}
}

// TestAcquireRelease_PoolSaturation tests pool saturation and recovery
func TestAcquireRelease_PoolSaturation(t *testing.T) {
	maxOpen := 3

	conn, err := Open(&Options{
		Addr:            []string{"localhost:9000"},
		DialTimeout:     time.Second,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		DialStrategy: func(ctx context.Context, connID int, opt *Options, dial Dial) (DialResult, error) {
			return DialResult{conn: newMockTransport(connID)}, nil
		},
	})
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer conn.Close()

	ch := conn.(*clickhouse)

	// Saturate the pool
	conns := make([]nativeTransport, maxOpen)
	for i := 0; i < maxOpen; i++ {
		transport, err := ch.acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire %d failed: %v", i, err)
		}
		conns[i] = transport
	}

	// Release one
	ch.release(conns[0], nil)

	// Should now be able to acquire again
	transport, err := ch.acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}

	if transport.connID() != conns[0].connID() {
		t.Error("expected to reuse released connection")
	}
}
