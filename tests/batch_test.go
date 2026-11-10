package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestBatchContextCancellation(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, conn.Exec(context.Background(), "create table if not exists test_batch_cancellation (x String) engine=MergeTree ORDER BY tuple()"))
	defer conn.Exec(context.Background(), "drop table if exists test_batch_cancellation")

	b, err := conn.PrepareBatch(ctx, "insert into test_batch_cancellation")
	require.NoError(t, err)
	for i := 0; i < 1_000_000; i++ {
		require.NoError(t, b.Append("value"))
	}

	cancel()

	require.Error(t, b.Send(), context.DeadlineExceeded.Error())

	// assert if connection is properly released after context cancellation
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}

func TestBatchCloseConnectionReleased(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	b, err := conn.PrepareBatch(context.Background(), "INSERT INTO function null('x UInt64')")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, b.Append(i))
	}

	err = b.Close()
	require.NoError(t, err)

	// assert if connection is properly released after close called
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}

func TestBatchSendConnectionReleased(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	b, err := conn.PrepareBatch(context.Background(), "INSERT INTO function null('x UInt64')")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, b.Append(i))
	}

	err = b.Send()
	require.NoError(t, err)

	// Close should be deferred after the batch is opened
	// Validate that it can be called after Send
	err = b.Close()
	require.NoError(t, err)

	// assert if connection is properly released after Send called
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}

// This test validates that Close() releases the connection even when closeQuery() fails
// due to a cancelled context. Before the fix, Close() would return the error from
// closeQuery() without calling release(), causing a connection pool leak.
func TestBatchCloseConnectionReleasedOnError(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	opts.DialTimeout = 2 * time.Second
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	b, err := conn.PrepareBatch(ctx, "INSERT INTO function null('x UInt64')")
	require.NoError(t, err)
	require.NoError(t, b.Append(1))

	// Cancel the context so that closeQuery() will fail inside Close()
	cancel()

	// Close should return an error (from closeQuery), but still release the connection
	require.Error(t, b.Close())

	// If the connection was properly released, we can acquire it again.
	// Without the fix, this would timeout because the connection slot was leaked.
	require.NoError(t, conn.Exec(context.Background(), "SELECT 1"))
}

// This test validates that connections are blocked if a batch is not properly
// cleaned up. This isn't required behavior, but this test confirms it happens.
func TestBatchCloseConnectionHold(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	opts.DialTimeout = 2 * time.Second // Lower timeout for faster acquire error
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	b, err := conn.PrepareBatch(context.Background(), "INSERT INTO function null('x UInt64')")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, b.Append(i))
	}

	// batch.Close() should be called here

	// assert if connection is blocked if close is not called.
	require.ErrorIs(t, conn.Exec(context.Background(), "SELECT 1"), clickhouse.ErrAcquireConnTimeout)
}

// TestBatchSendRetryWatchdogConnRace validates that the Send context watchdog
// does not touch b.conn while the retry path in Send replaces it. A second Send
// on a batch that was already sent goes through resetConnection, which assigns
// b.conn. Before the fix, the watchdog was armed before that assignment, so
// with an already-cancelled context its goroutine read b.conn concurrently
// (a data race under -race). It could also close the previous connection,
// which Send had already returned to the pool.
func TestBatchSendRetryWatchdogConnRace(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	opts.DialTimeout = 2 * time.Second
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())

		b, err := conn.PrepareBatch(ctx, "INSERT INTO function null('x UInt64')")
		require.NoError(t, err, "round %d: prepare batch", i)
		require.NoError(t, b.Append(uint64(i)), "round %d: append", i)
		require.NoError(t, b.Send(), "round %d: first send", i)

		// The watchdog of the next Send sees a done context at once and races
		// with resetConnection.
		cancel()
		require.ErrorIs(t, b.Send(), context.Canceled, "round %d: retry send with a cancelled context", i)

		// The connection pool must still be usable.
		require.NoError(t, conn.Exec(context.Background(), "SELECT 1"), "round %d: query after retry send", i)
	}
}
