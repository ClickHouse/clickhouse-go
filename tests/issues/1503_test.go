//go:build !race

package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestIssue1503(t *testing.T) {
	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Settings gets re-used between contexts
	settings := clickhouse.Settings{
		"async_insert": "0",
	}

	// Try to force a concurrent map write error from re-using the same settings map
	var wg sync.WaitGroup
	testInsert := func() {
		ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settings))
		err = conn.Exec(ctx, "INSERT INTO function null('x UInt64') VALUES (1)")
		require.NoError(t, err)
		wg.Done()
	}

	attempts := 10
	concurrency := 5

	for i := 0; i < attempts; i++ {
		wg.Add(concurrency)
		for j := 0; j < concurrency; j++ {
			go testInsert()
		}

		wg.Wait()
	}
}
