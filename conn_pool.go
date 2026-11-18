package clickhouse

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/internal/circular"
)

var errQueueEmpty = errors.New("clickhouse: connection pool queue is empty")

type connPool struct {
	mu    sync.RWMutex
	conns *circular.Queue[nativeTransport]

	// tick drives the periodic drain of expired connections. stopTick
	// releases the resources behind tick. Tests can inject their own tick
	// channel through newConnPoolWithTick to control when a drain occurs.
	tick     <-chan time.Time
	stopTick func()

	// finish is closed by the first Close to tell runDrainPool to exit.
	// finished is closed by runDrainPool when it exits.
	// drained is closed by the first Close after the final drain, so every
	// Close call returns only after all pooled connections are closed.
	finish   chan struct{}
	finished chan struct{}
	drained  chan struct{}
	// closing is set by the first Close. Guarded by mu.
	closing bool

	maxConnLifetime time.Duration
}

func newConnPool(lifetime time.Duration, capacity int) *connPool {
	ticker := time.NewTicker(lifetime)
	return newConnPoolWithTick(lifetime, capacity, ticker.C, ticker.Stop)
}

// newConnPoolWithTick creates a pool whose drain goroutine runs on each
// value received from tick. stopTick is called when the drain goroutine exits.
func newConnPoolWithTick(lifetime time.Duration, capacity int, tick <-chan time.Time, stopTick func()) *connPool {
	pool := &connPool{
		conns:           circular.New[nativeTransport](capacity),
		tick:            tick,
		stopTick:        stopTick,
		finish:          make(chan struct{}),
		finished:        make(chan struct{}),
		drained:         make(chan struct{}),
		maxConnLifetime: lifetime,
	}

	// runDrainPool exits when Close closes finish.
	go pool.runDrainPool()

	return pool
}

func (i *connPool) Len() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.conns.Len()
}

func (i *connPool) Cap() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.conns.Cap()
}

func (i *connPool) Get(ctx context.Context) (nativeTransport, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// check if pool was closed, or started to close, while we waited on the
	// lock. Close releases the lock while it waits for runDrainPool, so
	// closing can be set before closed() reports true.
	if i.closed() || i.closing {
		return nil, ErrConnectionClosed
	}

	// this loop continues until either:
	// a) the provided context is cancelled
	// b) the underlying circular queue is empty
	// c) it finds a non-expired connection
	for {
		if err := ctx.Err(); err != nil {
			// context has been cancelled
			return nil, context.Cause(ctx)
		}

		// Try to pull a connection
		conn, ok := i.conns.Pull()
		if !ok {
			return nil, errQueueEmpty // queue is empty
		}

		if !i.isExpired(conn) {
			return conn, nil
		}

		i.logExpired(conn, "closing expired connection from pool")
		conn.close()
	}
}

func (i *connPool) Put(conn nativeTransport) {
	if i.isExpired(conn) {
		i.logExpired(conn, "connection not returned to pool: lifetime expired")
		conn.close()
		return
	}

	if err := conn.healthCheck(); err != nil {
		conn.getLogger().Debug("connection not returned to pool: connection is bad", slog.Any("reason", err))
		conn.close()
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed() || i.closing {
		conn.getLogger().Debug("connection not returned to pool: pool is closed")
		conn.close()
		return
	}

	// Try to push the connection
	if !i.conns.Push(conn) {
		// Buffer is full, close the connection
		conn.getLogger().Debug("connection not returned to pool: pool is full")
		conn.close()
	}
}

func (i *connPool) Close() error {
	i.mu.Lock()
	if i.closing {
		// Another Close started first. Wait until it has closed all pooled
		// connections, so every caller sees the same result.
		i.mu.Unlock()
		<-i.drained
		return nil
	}
	i.closing = true
	close(i.finish)
	// Release the mutex before waiting for the drain goroutine. Holding it
	// across <-finished deadlocks if that goroutine is blocked on Lock in
	// the tick branch.
	i.mu.Unlock()

	<-i.finished

	i.mu.Lock()
	defer i.mu.Unlock()
	i.drainPool()
	close(i.drained)

	return nil
}

func (i *connPool) closed() bool {
	select {
	case <-i.finished:
		return true
	default:
		return false
	}
}

func (i *connPool) runDrainPool() {
	defer func() {
		i.stopTick()
		close(i.finished)
	}()

	for {
		select {
		case <-i.tick:
			i.mu.Lock()
			if i.closing {
				i.mu.Unlock()
				return
			}
			i.drainPool()
			i.mu.Unlock()
		case <-i.finish:
			return
		}
	}
}

// drainPool removes connections from the pool.
// If the pool is closed, it removes all connections.
// Otherwise, it only removes expired connections.
// Must be called with i.mu held.
func (i *connPool) drainPool() {
	if i.closed() {
		// Close all connections
		for conn := range i.conns.Clear() {
			conn.close()
		}
		return
	}

	// Remove only expired connections
	for conn := range i.conns.DeleteFunc(func(conn nativeTransport) bool {
		return i.isExpired(conn)
	}) {
		i.logExpired(conn, "closing expired connection from pool")
		conn.close()
	}
}

func (i *connPool) isExpired(conn nativeTransport) bool {
	return !time.Now().Before(i.expires(conn))
}

func (i *connPool) logExpired(conn nativeTransport, msg string) {
	conn.getLogger().Debug(msg,
		slog.Duration("age", time.Since(conn.connectedAtTime())),
		slog.Duration("max_lifetime", i.maxConnLifetime),
	)
}

func (i *connPool) expires(conn nativeTransport) time.Time {
	return conn.connectedAtTime().Add(i.maxConnLifetime)
}
