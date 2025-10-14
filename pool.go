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

	finish   chan struct{}
	finished chan struct{}

	maxConnLifetime time.Duration
}

func newIdlePool(lifetime time.Duration, capacity int) *idlePool {
	pool := &idlePool{
		conns:    make(conns, 0, capacity),
		finish:   make(chan struct{}),
		finished: make(chan struct{}),

		maxConnLifetime: lifetime,
	}

	go func() {
		ticker := time.NewTicker(lifetime)
		defer func() {
			ticker.Stop()
			close(pool.finished)
		}()

		for {
			select {
			case <-ticker.C:
				pool.mu.Lock()
				pool.drainPool()
				pool.mu.Unlock()
			case <-pool.finish:
				return
			}
		}
	}()

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

		if !i.expired(conn) {
			return conn
		}

		conn.close()
	}
}

func (i *idlePool) Put(conn nativeTransport) {
	if i.expired(conn) {
		return
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed() {
		return
	}

	// skip adding the connection if it is older
	// than the earliest connected at in the pool
	if len(i.conns) > 0 && conn.connectedAtTime().
		Before(i.conns[0].connectedAtTime()) {
		return
	}

	// remove the current minimum if the pool
	// is at capacity
	if len(i.conns) == cap(i.conns) {
		heap.Pop(&i.conns)
	}

	heap.Push(&i.conns, conn)
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

// drainPool removes connections from the pool.
// If the pool is closed, it removes all connections.
// Otherwise, it only removes expired connections.
// Must be called with i.mu held.
func (i *idlePool) drainPool() {
	closed := i.closed()

	for i.conns.Len() > 0 {
		conn, ok := heap.Pop(&i.conns).(nativeTransport)
		if !ok {
			return
		}

		// If pool is closed, drain all connections
		// Otherwise, push back non-expired connection and return
		if !closed && !i.expired(conn) {
			heap.Push(&i.conns, conn)
			return
		}

		conn.close()
	}
}

func (i *idlePool) expired(conn nativeTransport) bool {
	cutoff := conn.connectedAtTime().Add(i.maxConnLifetime)
	return time.Now().After(cutoff)
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
