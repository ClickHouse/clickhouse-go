package issues

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func Test548(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout: time.Second,
	})

	defer cancel()
	assert.NoError(t, err)
	// give it plenty of time before we conclusively assume deadlock
	timeout := time.After(5 * time.Second)
	done := make(chan bool)
	go func() {
		// should take 1s
		rows, _ := conn.Query(ctx, "SELECT sleepEachRow(0.001) as Col1 FROM system.numbers LIMIT 1000 SETTINGS max_block_size=10;")
		rows.Close()
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("Close() deadlocked")
	case <-done:
	}
}
