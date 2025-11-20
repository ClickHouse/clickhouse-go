//go:build go1.25

package clickhouse

import (
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
)

// ConnectionPoolCleanupLeak demonstrates that drainPool
// goroutines are not leaked when connections are opened and closed.
//
// This test uses Go's synctest package (stably available in Go 1.25+) to deterministically
// detect the leaked goroutine without relying on timing or sleep calls.
func TestConnectionPoolCleanupLeak(t *testing.T) {
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
		// This will hang if background goroutines are left behind.
		synctest.Wait()
	})
}

// TestConnectionPoolCleanupLeakConcurrent demonstrates that drainPool
// goroutines are not leaked when connections are opened and closed.
//
// This test uses Go's synctest package (stably available in Go 1.25+) to deterministically
// detect the leaked goroutine without relying on timing or sleep calls.
func TestConnectionPoolCleanupLeakConcurrent(t *testing.T) {
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
		// This will hang if background goroutines are left behind.
		synctest.Wait()
	})
}
