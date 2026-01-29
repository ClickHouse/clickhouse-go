package clickhouse

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func TestConnPool_Cap(t *testing.T) {
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
			pool := newConnPool(time.Hour, tt.capacity)
			defer pool.Close()

			assert.Equal(t, tt.capacity, pool.Cap())
		})
	}
}

func TestConnPool_Len(t *testing.T) {
	pool := newConnPool(time.Hour, 5)
	defer pool.Close()

	assert.Equal(t, 0, pool.Len(), "new pool should have length 0")

	// Add connections
	conn1 := &mockTransport{connectedAt: time.Now(), id: 1}
	pool.Put(conn1)
	assert.Equal(t, 1, pool.Len())

	conn2 := &mockTransport{connectedAt: time.Now(), id: 2}
	pool.Put(conn2)
	assert.Equal(t, 2, pool.Len())

	// Remove connection
	ctx := context.Background()
	conn, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, 1, pool.Len())

	conn, err = pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, conn)
	assert.Equal(t, 0, pool.Len())
}

func TestConnPool_GetEmpty(t *testing.T) {
	pool := newConnPool(time.Hour, 5)
	defer pool.Close()

	ctx := context.Background()
	conn, err := pool.Get(ctx)
	require.ErrorIs(t, err, errQueueEmpty)
	assert.Nil(t, conn, "getting from empty pool should return nil")
}

func TestConnPool_PutAndGet(t *testing.T) {
	pool := newConnPool(time.Hour, 5)
	defer pool.Close()

	now := time.Now()
	conn1 := &mockTransport{connectedAt: now, id: 1}
	conn2 := &mockTransport{connectedAt: now.Add(time.Second), id: 2}
	conn3 := &mockTransport{connectedAt: now.Add(2 * time.Second), id: 3}

	// Put connections
	pool.Put(conn1)
	pool.Put(conn2)
	pool.Put(conn3)

	assert.Equal(t, 3, pool.Len())

	ctx := context.Background()

	// Get should return oldest connection first (heap min)
	retrieved, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, 1, retrieved.connID(), "should get oldest connection first")

	retrieved, err = pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, 2, retrieved.connID())

	retrieved, err = pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, 3, retrieved.connID())

	// Pool should be empty now
	retrieved, err = pool.Get(ctx)
	require.ErrorIs(t, err, errQueueEmpty)
	assert.Nil(t, retrieved)
}

func TestConnPool_CapacityLimit(t *testing.T) {
	capacity := 3
	pool := newConnPool(time.Hour, capacity)
	defer pool.Close()

	now := time.Now()

	// Add more connections than capacity
	allConns := make([]*mockTransport, 5)
	for i := 0; i < 5; i++ {
		conn := &mockTransport{
			connectedAt: now.Add(time.Duration(i) * time.Second),
			id:          i + 1,
		}
		allConns[i] = conn
		pool.Put(conn)
	}

	// Pool should not exceed capacity
	assert.Equal(t, capacity, pool.Len())

	// Connections 4 and 5 should have been closed (rejected when pool at capacity)
	assert.True(t, allConns[3].closed, "connection 4 should be closed when pool is full")
	assert.True(t, allConns[4].closed, "connection 5 should be closed when pool is full")
	assert.False(t, allConns[0].closed, "connection 1 should not be closed")
	assert.False(t, allConns[1].closed, "connection 2 should not be closed")
	assert.False(t, allConns[2].closed, "connection 3 should not be closed")

	ctx := context.Background()

	// With FIFO insertion order, the first 3 connections are kept [1, 2, 3]
	// connections (4) and (5) are rejected when pool is at capacity
	for i := 0; i < 3; i++ {
		retrieved, err := pool.Get(ctx)
		require.NoError(t, err)
		require.NotNil(t, retrieved)

		assert.Equal(t, i+1, retrieved.connID(), "should get connections in FIFO order")
	}
}

func TestConnPool_ExpiredConnectionNotReturned(t *testing.T) {
	// Pool with very short lifetime
	lifetime := 100 * time.Millisecond
	pool := newConnPool(lifetime, 5)
	defer pool.Close()

	// Add connection that is not yet expired (but close to expiration)
	oldConn := &mockTransport{
		connectedAt: time.Now().Add(-50 * time.Millisecond),
		id:          1,
	}
	pool.Put(oldConn)

	// Add fresh connection
	freshConn := &mockTransport{
		connectedAt: time.Now(),
		id:          2,
	}
	pool.Put(freshConn)

	// Wait for the old connection to expire
	time.Sleep(60 * time.Millisecond)

	ctx := context.Background()

	// Get should skip expired connection and return fresh one
	retrieved, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	assert.Equal(t, 2, retrieved.connID(), "should skip expired connection")

	// The expired connection should have been closed during Get()
	assert.True(t, oldConn.closed, "expired connection should be closed")
	assert.False(t, freshConn.closed, "fresh connection should not be closed")

	// Pool should be empty now
	retrieved, err = pool.Get(ctx)
	require.ErrorIs(t, err, errQueueEmpty)
	assert.Nil(t, retrieved)
}

func TestConnPool_PutExpiredConnection(t *testing.T) {
	lifetime := 100 * time.Millisecond
	pool := newConnPool(lifetime, 5)
	defer pool.Close()

	// Try to put already expired connection
	expiredConn := &mockTransport{
		connectedAt: time.Now().Add(-200 * time.Millisecond),
		id:          1,
	}
	pool.Put(expiredConn)

	// Pool should not accept expired connection
	assert.Equal(t, 0, pool.Len())
}

func TestConnPool_PutOlderThanMinimumWithCapacity(t *testing.T) {
	pool := newConnPool(time.Hour, 5)
	defer pool.Close()

	now := time.Now()

	// Add a connection
	conn1 := &mockTransport{connectedAt: now, id: 1}
	pool.Put(conn1)

	// Add an older connection (but inserted second)
	olderConn := &mockTransport{connectedAt: now.Add(-time.Minute), id: 2}
	pool.Put(olderConn)

	// Both connections should be in the pool
	assert.Equal(t, 2, pool.Len())

	ctx := context.Background()
	retrieved, err := pool.Get(ctx)
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	// With FIFO insertion order, we get the first inserted connection
	assert.Equal(t, 1, retrieved.connID(), "should retrieve the first inserted connection")
}

func TestConnPool_GetWithCancelledContext(t *testing.T) {
	pool := newConnPool(time.Hour, 5)
	defer pool.Close()

	// Add a connection
	conn := &mockTransport{connectedAt: time.Now(), id: 1}
	pool.Put(conn)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Get with cancelled context should return context error
	retrieved, err := pool.Get(ctx)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
	assert.Nil(t, retrieved)

	// Connection should still be in pool
	assert.Equal(t, 1, pool.Len())
}

func TestConnPool_Close(t *testing.T) {
	pool := newConnPool(time.Hour, 5)

	// Add connections
	for i := 0; i < 3; i++ {
		conn := &mockTransport{
			connectedAt: time.Now(),
			id:          i + 1,
		}
		pool.Put(conn)
	}

	assert.Equal(t, 3, pool.Len())

	// Close the pool
	err := pool.Close()
	assert.NoError(t, err)

	// Verify pool is closed
	assert.True(t, pool.closed())

	// Get should return ErrConnectionClosed from closed pool
	ctx := context.Background()
	conn, err := pool.Get(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrConnectionClosed, err)
	assert.Nil(t, conn)

	// Put should be ignored on closed pool
	initialLen := pool.Len()
	newConn := &mockTransport{connectedAt: time.Now(), id: 99}
	pool.Put(newConn)
	assert.Equal(t, initialLen, pool.Len(), "closed pool should not accept new connections")

	// Closing again should be safe
	err = pool.Close()
	assert.NoError(t, err)
}

func TestConnPool_CloseWithDrain(t *testing.T) {
	pool := newConnPool(time.Hour, 5)

	// Add connections
	allConns := make([]*mockTransport, 3)
	for i := 0; i < 3; i++ {
		mock := &mockTransport{
			connectedAt: time.Now(),
			id:          i + 1,
		}
		allConns[i] = mock
		pool.Put(mock)
	}

	assert.Equal(t, 3, pool.Len(), "pool should have 3 connections before close")

	// Close the pool
	err := pool.Close()
	assert.NoError(t, err)

	// Verify pool is closed
	assert.True(t, pool.closed())

	// Verify all connections are drained from the pool
	assert.Equal(t, 0, pool.Len(), "pool should be empty after close (all connections drained)")

	// Verify all connections were closed
	for i, conn := range allConns {
		assert.True(t, conn.closed, "connection %d should be closed after pool close", i+1)
	}

	// Verify no connections can be retrieved
	ctx := context.Background()
	conn, err := pool.Get(ctx)
	assert.Error(t, err)
	assert.Equal(t, ErrConnectionClosed, err)
	assert.Nil(t, conn, "get should return nil after pool is closed and drained")
}

func TestConnPool_DrainExpiredConnections(t *testing.T) {
	lifetime := 100 * time.Millisecond
	pool := newConnPool(lifetime, 5)
	defer pool.Close()

	// Add connections that are already old (so they will definitely expire)
	oldTime := time.Now().Add(-50 * time.Millisecond)
	expiredConns := make([]*mockTransport, 3)
	for i := 0; i < 3; i++ {
		conn := &mockTransport{
			connectedAt: oldTime.Add(time.Duration(i) * time.Millisecond),
			id:          i + 1,
		}
		expiredConns[i] = conn
		pool.Put(conn)
	}

	assert.Equal(t, 3, pool.Len())

	// Wait for connections to expire and drain cycle to run
	// The connections will be 50ms + 100ms (sleep) = 150ms old, exceeding the 100ms lifetime
	time.Sleep(lifetime + 50*time.Millisecond)

	// At this point the drain should have run and removed the expired connections
	assert.Equal(t, 0, pool.Len(), "all expired connections should be drained")

	// Verify all expired connections were closed
	for i, conn := range expiredConns {
		assert.True(t, conn.closed, "expired connection %d should be closed", i+1)
	}

	// Add a fresh connection to verify pool still works after drain
	freshConn := &mockTransport{
		connectedAt: time.Now(),
		id:          99,
	}
	pool.Put(freshConn)

	// Fresh connection should be in the pool
	assert.Equal(t, 1, pool.Len(), "fresh connection should be added after drain")
	assert.False(t, freshConn.closed, "fresh connection should not be closed")
}

func TestConnPool_ConcurrentAccess(t *testing.T) {
	pool := newConnPool(time.Hour, 10)
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
			_, _ = pool.Get(ctx)
			time.Sleep(time.Millisecond)
		}
	}()

	// Wait for puts to complete
	<-done

	// Give gets time to complete
	time.Sleep(50 * time.Millisecond)

	// Pool should not exceed capacity
	assert.LessOrEqual(t, pool.Len(), pool.Cap())
}

func TestConnPool_FIFOOrdering(t *testing.T) {
	pool := newConnPool(time.Hour, 10)
	defer pool.Close()

	now := time.Now()

	// Add connections with varying timestamps in non-chronological order
	// to test that FIFO is based on insertion order, not connection age
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

	expectedCount := 5
	assert.Equal(t, expectedCount, pool.Len())

	ctx := context.Background()

	// Get all connections and verify they come out in insertion order (FIFO)
	for _, id := range []int{1, 5, 2, 4, 3} {
		conn, err := pool.Get(ctx)
		require.NoError(t, err)
		require.NotNil(t, conn, "should get connection %d", id)
		assert.Equal(t, id, conn.connID(),
			"connections should be returned in FIFO insertion order")
	}
}

// mockTransport implements nativeTransport for testing
type mockTransport struct {
	connectedAt   time.Time
	id            int
	released      bool
	closed        bool
	bad           bool
	bufferFreed   bool
	debugMessages []string
	mu            sync.Mutex
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

func (m *mockTransport) uploadFile(ctx context.Context, reader io.Reader, query string) error {
	return nil
}

func (m *mockTransport) asyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	return nil
}

func (m *mockTransport) ping(ctx context.Context) error {
	return nil
}

func (m *mockTransport) isBad() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bad
}

func (m *mockTransport) connID() int {
	return m.id
}

func (m *mockTransport) connectedAtTime() time.Time {
	return m.connectedAt
}

func (m *mockTransport) isReleased() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.released
}

func (m *mockTransport) setReleased(released bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released = released
}

func (m *mockTransport) getLogger() *slog.Logger {
	return newNoopLogger()
}

func (m *mockTransport) freeBuffer() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bufferFreed = true
}

func (m *mockTransport) close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// Helper methods for testing
func newMockTransport(id int) *mockTransport {
	return &mockTransport{
		connectedAt: time.Now(),
		id:          id,
	}
}

func (m *mockTransport) setBad(bad bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bad = bad
}

func (m *mockTransport) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closed
}

func (m *mockTransport) wasBufferFreed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.bufferFreed
}
