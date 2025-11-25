//go:build go1.25

package clickhouse

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnPool_ExpiredConnectionsAreDrained(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool := newConnPool(50*time.Millisecond, 5)
		defer pool.Close()

		firstConn := &mockTransport{
			connectedAt: time.Now(),
			id:          1,
		}
		pool.Put(firstConn)

		secondConn := &mockTransport{
			connectedAt: time.Now().Add(50 * time.Millisecond),
			id:          2,
		}
		pool.Put(secondConn)

		// Wait for the old connection to expire
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		// Ensure the first connection is already closed before we attempt
		// to Get one from the pool. This is to ensure that it was closed by
		// the background drail pool and not on the call to Get.
		assert.True(t, firstConn.closed, "first connection should be closed")

		ctx := context.Background()
		// Get should skip expired connection and return fresh one
		retrieved, err := pool.Get(ctx)
		require.NoError(t, err)
		require.NotNil(t, retrieved)
		assert.Equal(t, 2, retrieved.connID(), "should skip expired connection")

		assert.False(t, secondConn.closed, "second connection should not be closed")

		// put the second connection back so that it can be expired by background routine
		pool.Put(secondConn)

		// Wait for the second connection to expire
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()

		// Again ensure the connection has been closed due to the drain
		// pool goroutine and not the next call to Get.
		assert.True(t, secondConn.closed, "second connection should be closed")

		// Pool should be empty now
		retrieved, err = pool.Get(ctx)
		require.ErrorIs(t, err, errQueueEmpty)
		assert.Nil(t, retrieved)

		assert.Nil(t, pool.Close())
		synctest.Wait()
	})
}
