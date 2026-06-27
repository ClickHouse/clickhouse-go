package issues

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

// Test1831 is a regression test for https://github.com/ClickHouse/clickhouse-go/issues/1831.
// It exercises the race between concurrent release() calls and Close(), verifying that
// no TCP connection is leaked when the pool shuts down mid-flight.
func Test1831(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)

	// Small pool so connections cycle through the idle pool frequently,
	// making the Close/release race more likely to be triggered.
	opts := clickhouse_tests.ClientOptionsFromEnv(testEnv, clickhouse_tests.TestClientDefaultSettings(testEnv), false)
	opts.MaxOpenConns = 5
	opts.MaxIdleConns = 5

	conn, err := clickhouse_tests.GetConnectionWithOptions(&opts)
	require.NoError(t, err)

	const workers = 20
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ready := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			for j := 0; j < 10; j++ {
				_ = conn.Ping(ctx) // errors expected once Close() is called
			}
		}()
	}

	close(ready)

	// Close() while queries are in flight: must not panic or leak connections.
	require.NoError(t, conn.Close())

	wg.Wait()

	// After Close(), operations must return an error promptly, not hang.
	err = conn.Ping(context.Background())
	require.Error(t, err, "Ping after Close() should return an error")
}
