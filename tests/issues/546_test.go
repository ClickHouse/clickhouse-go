package issues

import (
	"context"
	"crypto/tls"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func Test546(t *testing.T) {
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
	conn, err := clickhouse_tests.GetConnectionWithOptions(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		TLS:             tlsConfig,
	})
	require.NoError(t, err)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 2000000,
	}),
		clickhouse.WithProgress(func(p *clickhouse.Progress) {
			fmt.Println("progress: ", p)
		}), clickhouse.WithProfileInfo(func(p *clickhouse.ProfileInfo) {
			fmt.Println("profile info: ", p)
		}))
	require.NoError(t, conn.Ping(ctx))
	if exception, ok := err.(*clickhouse.Exception); ok {
		fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
	}
	assert.NoError(t, err)

	rows, err := conn.Query(ctx, "SELECT * FROM system.numbers LIMIT 2000000", time.Now())
	assert.NoError(t, err)
	i := 0
	for rows.Next() {
		var (
			col1 uint64
		)
		if err := rows.Scan(&col1); err != nil {
			assert.NoError(t, err)
		}
		i += 1
	}
	assert.NoError(t, rows.Err())
	assert.Equal(t, 2000000, i)
	rows.Close()
}
