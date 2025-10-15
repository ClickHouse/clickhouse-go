package clickhouse

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type idlePool struct {
	mu    sync.RWMutex
	conns conns

	ticker   *time.Ticker
	finish   chan struct{}
	finished chan struct{}

	maxConnLifetime time.Duration
}

func newIdlePool(lifetime time.Duration, capacity int) *idlePool {
	pool := &idlePool{
		conns:    make(conns, 0, capacity),
		ticker:   time.NewTicker(lifetime),
		finish:   make(chan struct{}),
		finished: make(chan struct{}),

		maxConnLifetime: lifetime,
	}

	go pool.runDrainPool()

	return pool
}

func (i *idlePool) Length() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.conns.Len()
}

func (i *idlePool) Capacity() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return cap(i.conns)
}

func (i *idlePool) Get(ctx context.Context) nativeTransport {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed() {
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			// context has been cancelled
			return nil
		}

		if i.closed() {
			return nil
		}

		if len(i.conns) == 0 {
			return nil
		}

		conn, ok := heap.Pop(&i.conns).(nativeTransport)
		if !ok {
			return nil
		}

		if !i.isExpired(conn) {
			return conn
		}

		conn.close()
	}
}

func (i *idlePool) Put(conn nativeTransport) {
	if i.isExpired(conn) || conn.isBad() {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed() {
		return
	}

	// quick path: if connections is empty then
	// simply insert it as the new minimum
	if len(i.conns) == 0 {
		i.conns = append(i.conns, conn)
		i.updateTicker()
		return
	}

	// skip adding the connection if it is older
	// than the earliest connected at in the pool
	if conn.connectedAtTime().
		Before(i.conns[0].connectedAtTime()) {
		conn.close()
		return
	}

	curMinConnected := i.conns[0].connectedAtTime()

	// remove the current minimum if the pool
	// is at capacity
	if len(i.conns) == cap(i.conns) {
		heap.Pop(&i.conns)
	}

	heap.Push(&i.conns, conn)

	// update ticker if the pools minimum has changed
	if curMinConnected != i.conns[0].connectedAtTime() {
		i.updateTicker()
	}
}

func (i *idlePool) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed() {
		return nil
	}

	close(i.finish)

	<-i.finished

	// Drain all remaining connections from the pool
	i.drainPool()

	return nil
}

func (i *idlePool) closed() bool {
	select {
	case <-i.finished:
		return true
	default:
		return false
	}
}

func (i *idlePool) runDrainPool() {
	defer func() {
		i.ticker.Stop()
		close(i.finished)
	}()

	for {
		select {
		case <-i.ticker.C:
			i.mu.Lock()
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
func (i *idlePool) drainPool() {
	defer i.updateTicker()

	closed := i.closed()

	for i.conns.Len() > 0 {
		// If pool is closed, drain all connections
		// Otherwise, continue to drain until the oldest
		// connection is non-expired
		if !closed && !i.isExpired(i.conns[0]) {
			return
		}

		conn, ok := heap.Pop(&i.conns).(nativeTransport)
		if !ok {
			return
		}

		conn.close()
	}
}

// updateTicker resets the tickers next tick to be when the
// current minimum connection is due to expire.
// Must be called with i.mu held.
func (i *idlePool) updateTicker() {
	if len(i.conns) == 0 {
		i.ticker.Reset(i.maxConnLifetime)
		return
	}

	if expiresIn := i.expires(i.conns[0]).Sub(time.Now()); expiresIn > 0 {
		i.ticker.Reset(expiresIn)
	}
}

func (i *idlePool) isExpired(conn nativeTransport) bool {
	return time.Now().After(i.expires(conn))
}

func (i *idlePool) expires(conn nativeTransport) time.Time {
	return conn.connectedAtTime().Add(i.maxConnLifetime)
}

type conns []nativeTransport

// Len is the number of elements in the collection.
func (c conns) Len() int {
	return len(c)
}

// Less reports whether the element with index i
// must sort before the element with index j.
func (c conns) Less(i int, j int) bool {
	return c[i].connectedAtTime().Before(c[j].connectedAtTime())
}

// Swap swaps the elements with indexes i and j.
func (c conns) Swap(i int, j int) {
	c[i], c[j] = c[j], c[i]
}

// Push appends the entry to the end of the underlying slice.
func (c *conns) Push(x any) {
	*c = append(*c, x.(nativeTransport))
}

// Pop removes and returns the last element in the underlying slice.
func (c *conns) Pop() any {
	old := *c
	n := len(old)
	x := old[n-1]
	*c = old[0 : n-1]
	return x
}
