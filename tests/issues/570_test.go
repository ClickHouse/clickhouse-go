package issues

import (
	"crypto/tls"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
	"time"
)

func TestIssue570(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/default", env.Username, env.Password,
		env.Host, env.Port)
	port := env.Port
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
		dsn = fmt.Sprintf("clickhouse://%s:%s@%s:%d/default?secure=true", env.Username, env.Password,
			env.Host, env.SslPort)
	}
	require.NoError(t, err)
	// using ParseDNS - defaults shouldn't be set for maxOpenConnections etc
	options, err := clickhouse.ParseDSN(dsn)
	assert.NoError(t, err)
	conn := clickhouse_std_tests.GetConnectionWithOptions(options)
	conn.SetMaxOpenConns(5)
	conn.SetMaxIdleConns(10)
	assert.NoError(t, conn.Ping())
	conn.Close()

	// check we can pass Options
	options = &clickhouse.Options{
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
	}
	conn = clickhouse_std_tests.GetConnectionWithOptions(options)
	assert.NoError(t, conn.Ping())

	// check we can open with a DSN
	conn, err = sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	assert.NoError(t, conn.Ping())
}
