package issues

import (
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test570(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	// using ParseDNS - defaults shouldn't be set for maxOpenConnections etc
	options, err := clickhouse.ParseDSN(fmt.Sprintf("clickhouse://%s:%s@%s:%d/default", env.Username, env.Password,
		env.Host, env.Port))
	assert.NoError(t, err)
	conn := clickhouse.OpenDB(options)
	conn.SetMaxOpenConns(5)
	conn.SetMaxIdleConns(10)
	assert.NoError(t, conn.Ping())
	conn.Close()

	// check we can pass Options
	options = &clickhouse.Options{
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
	}
	conn = clickhouse.OpenDB(options)
	assert.NoError(t, conn.Ping())

	// check we can open with a DSN
	conn, err = sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s:%d", env.Username, env.Password,
		env.Host, env.Port))
	require.NoError(t, err)
	assert.NoError(t, conn.Ping())
}
