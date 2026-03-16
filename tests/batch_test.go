package tests

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

func TestBatchContextCancellation(t *testing.T) {
	te, err := GetTestEnvironment(testSet)
	require.NoError(t, err)
	opts := ClientOptionsFromEnv(te, clickhouse.Settings{}, false)
	opts.MaxOpenConns = 1
	conn, err := GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, conn.Exec(context.Background(), "create table if not exists test_batch_cancellation (x String) engine=Memory"))
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
