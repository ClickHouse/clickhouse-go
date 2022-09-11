package issues

import (
	"context"
	"crypto/tls"
	"fmt"
	clickhouse_tests "github.com/rnbondarenko/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/rnbondarenko/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func Test548(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	port := env.Port
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout: time.Second,
		TLS:         tlsConfig,
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
