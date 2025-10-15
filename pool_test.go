package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdlePool_Capacity(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
	}{
		{
			name:     "capacity 1",
			capacity: 1,
		},
		{
			name:     "capacity 5",
			capacity: 5,
		},
		{
			name:     "capacity 10",
			capacity: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newIdlePool(time.Hour, tt.capacity)
			defer pool.Close()

			assert.Equal(t, tt.capacity, pool.Capacity())
		})
	}
}

func TestIdlePool_Length(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)
	defer pool.Close()

	assert.Equal(t, 0, pool.Length(), "new pool should have length 0")

	// Add connections
	conn1 := &mockTransport{connectedAt: time.Now(), id: 1}
	pool.Put(conn1)
	assert.Equal(t, 1, pool.Length())

	conn2 := &mockTransport{connectedAt: time.Now(), id: 2}
	pool.Put(conn2)
	assert.Equal(t, 2, pool.Length())

	// Remove connection
	ctx := context.Background()
	pool.Get(ctx)
	assert.Equal(t, 1, pool.Length())

	pool.Get(ctx)
	assert.Equal(t, 0, pool.Length())
}

func TestIdlePool_GetEmpty(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)
	defer pool.Close()

	ctx := context.Background()
	conn := pool.Get(ctx)
	assert.Nil(t, conn, "getting from empty pool should return nil")
}

func TestIdlePool_PutAndGet(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)
	defer pool.Close()

	now := time.Now()
	conn1 := &mockTransport{connectedAt: now, id: 1}
	conn2 := &mockTransport{connectedAt: now.Add(time.Second), id: 2}
	conn3 := &mockTransport{connectedAt: now.Add(2 * time.Second), id: 3}

	// Put connections
	pool.Put(conn1)
	pool.Put(conn2)
	pool.Put(conn3)

	assert.Equal(t, 3, pool.Length())

	ctx := context.Background()

	// Get should return oldest connection first (heap min)
	retrieved := pool.Get(ctx)
	require.NotNil(t, retrieved)
	assert.Equal(t, 1, retrieved.connID(), "should get oldest connection first")

	retrieved = pool.Get(ctx)
	require.NotNil(t, retrieved)
	assert.Equal(t, 2, retrieved.connID())

	retrieved = pool.Get(ctx)
	require.NotNil(t, retrieved)
	assert.Equal(t, 3, retrieved.connID())

	// Pool should be empty now
	retrieved = pool.Get(ctx)
	assert.Nil(t, retrieved)
}

func TestIdlePool_CapacityLimit(t *testing.T) {
	capacity := 3
	pool := newIdlePool(time.Hour, capacity)
	defer pool.Close()

	now := time.Now()

	// Add more connections than capacity
	for i := 0; i < 5; i++ {
		conn := &mockTransport{
			connectedAt: now.Add(time.Duration(i) * time.Second),
			id:          i + 1,
		}
		pool.Put(conn)
	}

	// Pool should not exceed capacity
	assert.Equal(t, capacity, pool.Length())

	ctx := context.Background()

	// Expect the pool to return connections [3, 4, 5] in order
	// connections (1) and (2) will have been evicted due to capacity
	for i := 0; i < 3; i++ {
		retrieved := pool.Get(ctx)
		require.NotNil(t, retrieved)

		assert.True(t, retrieved.connID() == 3+i, "unexpected connection ID")
	}
}

func TestIdlePool_ExpiredConnectionNotReturned(t *testing.T) {
	// Pool with very short lifetime
	lifetime := 100 * time.Millisecond
	pool := newIdlePool(lifetime, 5)
	defer pool.Close()

	// Add connection that will expire
	oldConn := &mockTransport{
		connectedAt: time.Now().Add(-200 * time.Millisecond),
		id:          1,
	}
	pool.Put(oldConn)

	// Add fresh connection
	freshConn := &mockTransport{
		connectedAt: time.Now(),
		id:          2,
	}
	pool.Put(freshConn)

	ctx := context.Background()

	// Get should skip expired connection and return fresh one
	retrieved := pool.Get(ctx)
	require.NotNil(t, retrieved)
	assert.Equal(t, 2, retrieved.connID(), "should skip expired connection")

	// Pool should be empty now
	retrieved = pool.Get(ctx)
	assert.Nil(t, retrieved)
}

func TestIdlePool_PutExpiredConnection(t *testing.T) {
	lifetime := 100 * time.Millisecond
	pool := newIdlePool(lifetime, 5)
	defer pool.Close()

	// Try to put already expired connection
	expiredConn := &mockTransport{
		connectedAt: time.Now().Add(-200 * time.Millisecond),
		id:          1,
	}
	pool.Put(expiredConn)

	// Pool should not accept expired connection
	assert.Equal(t, 0, pool.Length())
}

func TestIdlePool_PutOlderThanMinimumWithCapacity(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)
	defer pool.Close()

	now := time.Now()

	// Add a connection
	conn1 := &mockTransport{connectedAt: now, id: 1}
	pool.Put(conn1)

	// Try to add an older connection that current minimum
	olderConn := &mockTransport{connectedAt: now.Add(-time.Minute), id: 2}
	pool.Put(olderConn)

	// Pool should insert connection into min
	assert.Equal(t, 2, pool.Length())

	ctx := context.Background()
	retrieved := pool.Get(ctx)
	require.NotNil(t, retrieved)
	assert.Equal(t, 2, retrieved.connID(), "should retrieve the oldest connection")
}

func TestIdlePool_GetWithCancelledContext(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)
	defer pool.Close()

	// Add a connection
	conn := &mockTransport{connectedAt: time.Now(), id: 1}
	pool.Put(conn)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Get with cancelled context should return nil
	retrieved := pool.Get(ctx)
	assert.Nil(t, retrieved)

	// Connection should still be in pool
	assert.Equal(t, 1, pool.Length())
}

func TestIdlePool_Close(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)

	// Add connections
	for i := 0; i < 3; i++ {
		conn := &mockTransport{
			connectedAt: time.Now(),
			id:          i + 1,
		}
		pool.Put(conn)
	}

	assert.Equal(t, 3, pool.Length())

	// Close the pool
	err := pool.Close()
	assert.NoError(t, err)

	// Verify pool is closed
	assert.True(t, pool.closed())

	// Get should return nil from closed pool
	ctx := context.Background()
	conn := pool.Get(ctx)
	assert.Nil(t, conn)

	// Put should be ignored on closed pool
	initialLen := pool.Length()
	newConn := &mockTransport{connectedAt: time.Now(), id: 99}
	pool.Put(newConn)
	assert.Equal(t, initialLen, pool.Length(), "closed pool should not accept new connections")

	// Closing again should be safe
	err = pool.Close()
	assert.NoError(t, err)
}

func TestIdlePool_CloseWithDrain(t *testing.T) {
	pool := newIdlePool(time.Hour, 5)

	// Add connections
	for i := 0; i < 3; i++ {
		mock := &mockTransport{
			connectedAt: time.Now(),
			id:          i + 1,
		}
		pool.Put(mock)
	}

	assert.Equal(t, 3, pool.Length(), "pool should have 3 connections before close")

	// Close the pool
	err := pool.Close()
	assert.NoError(t, err)

	// Verify pool is closed
	assert.True(t, pool.closed())

	// Verify all connections are drained from the pool
	assert.Equal(t, 0, pool.Length(), "pool should be empty after close (all connections drained)")

	// Verify no connections can be retrieved
	ctx := context.Background()
	conn := pool.Get(ctx)
	assert.Nil(t, conn, "get should return nil after pool is closed and drained")
}

func TestIdlePool_DrainExpiredConnections(t *testing.T) {
	lifetime := 100 * time.Millisecond
	pool := newIdlePool(lifetime, 5)
	defer pool.Close()

	// Add connections that are already old (so they will definitely expire)
	oldTime := time.Now().Add(-50 * time.Millisecond)
	for i := 0; i < 3; i++ {
		conn := &mockTransport{
			connectedAt: oldTime.Add(time.Duration(i) * time.Millisecond),
			id:          i + 1,
		}
		pool.Put(conn)
	}

	assert.Equal(t, 3, pool.Length())

	// Wait for connections to expire and drain cycle to run
	// The connections will be 50ms + 100ms (sleep) = 150ms old, exceeding the 100ms lifetime
	time.Sleep(lifetime + 50*time.Millisecond)

	// At this point the drain should have run and removed the expired connections
	assert.Equal(t, 0, pool.Length(), "all expired connections should be drained")

	// Add a fresh connection to verify pool still works after drain
	freshConn := &mockTransport{
		connectedAt: time.Now(),
		id:          99,
	}
	pool.Put(freshConn)

	// Fresh connection should be in the pool
	assert.Equal(t, 1, pool.Length(), "fresh connection should be added after drain")
}

func TestIdlePool_ConcurrentAccess(t *testing.T) {
	pool := newIdlePool(time.Hour, 10)
	defer pool.Close()

	ctx := context.Background()
	done := make(chan struct{})

	// Concurrent puts
	go func() {
		for i := 0; i < 20; i++ {
			conn := &mockTransport{
				connectedAt: time.Now(),
				id:          i,
			}
			pool.Put(conn)
			time.Sleep(time.Millisecond)
		}
		close(done)
	}()

	// Concurrent gets
	go func() {
		for i := 0; i < 20; i++ {
			pool.Get(ctx)
			time.Sleep(time.Millisecond)
		}
	}()

	// Wait for puts to complete
	<-done

	// Give gets time to complete
	time.Sleep(50 * time.Millisecond)

	// Pool should not exceed capacity
	assert.LessOrEqual(t, pool.Length(), pool.Capacity())
}

func TestIdlePool_HeapOrdering(t *testing.T) {
	pool := newIdlePool(time.Hour, 10)
	defer pool.Close()

	now := time.Now()

	// Add connections with incrementing timestamps
	// Adding them in non-chronological order to test heap sorting
	connections := []struct {
		time time.Time
		id   int
	}{
		{now.Add(1 * time.Second), 1},
		{now.Add(5 * time.Second), 5},
		{now.Add(2 * time.Second), 2},
		{now.Add(4 * time.Second), 4},
		{now.Add(3 * time.Second), 3},
	}

	// Add all connections
	for _, c := range connections {
		conn := &mockTransport{
			connectedAt: c.time,
			id:          c.id,
		}
		pool.Put(conn)
	}

	// Due to "older than minimum" logic, only connections >= first added will be kept
	// First added was "1 second", so all should be accepted
	expectedCount := 5
	assert.Equal(t, expectedCount, pool.Length())

	ctx := context.Background()
	var previousTime time.Time

	// Get all connections and verify they come out in time order (oldest first)
	for i := 0; i < expectedCount; i++ {
		conn := pool.Get(ctx)
		require.NotNil(t, conn, "should get connection %d", i)

		if i > 0 {
			assert.True(t, conn.connectedAtTime().After(previousTime) || conn.connectedAtTime().Equal(previousTime),
				"connections should be returned in time order (oldest first), got %v after %v",
				conn.connectedAtTime(), previousTime)
		}
		previousTime = conn.connectedAtTime()
	}
}

// mockTransport implements nativeTransport for testing
type mockTransport struct {
	connectedAt time.Time
	id          int
	released    bool
}

func (m *mockTransport) serverVersion() (*ServerVersion, error) {
	return nil, nil
}

func (m *mockTransport) query(ctx context.Context, release nativeTransportRelease, query string, args ...any) (*rows, error) {
	return nil, nil
}

func (m *mockTransport) queryRow(ctx context.Context, release nativeTransportRelease, query string, args ...any) *row {
	return nil
}

func (m *mockTransport) prepareBatch(ctx context.Context, release nativeTransportRelease, acquire nativeTransportAcquire, query string, opts driver.PrepareBatchOptions) (driver.Batch, error) {
	return nil, nil
}

func (m *mockTransport) exec(ctx context.Context, query string, args ...any) error {
	return nil
}

func (m *mockTransport) asyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return nil
}

func (m *mockTransport) ping(ctx context.Context) error {
	return nil
}

func (m *mockTransport) isBad() bool {
	return false
}

func (m *mockTransport) connID() int {
	return m.id
}

func (m *mockTransport) connectedAtTime() time.Time {
	return m.connectedAt
}

func (m *mockTransport) isReleased() bool {
	return m.released
}

func (m *mockTransport) setReleased(released bool) {
	m.released = released
}

func (m *mockTransport) debugf(format string, v ...any) {
	// no-op for testing
}

func (m *mockTransport) freeBuffer() {
	// no-op for testing
}

func (m *mockTransport) close() error {
	return nil
}
