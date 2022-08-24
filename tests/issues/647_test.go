package issues

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test647(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
	}
	conn, err := clickhouse.Open(options)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, conn.Ping(ctx))
	//reuse options
	conn2, err := clickhouse.Open(options)
	require.NoError(t, err)
	require.NoError(t, conn2.Ping(ctx))
}

func Test647_OpenDB(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
	}
	conn := clickhouse.OpenDB(options)
	require.NoError(t, conn.Ping())
	//reuse options
	conn2 := clickhouse.OpenDB(options)
	require.NoError(t, conn2.Ping())
	// allow nil to be parsed - should work if ClickHouse was available on 9000
	//conn3 := clickhouse.OpenDB(nil)
	//require.NoError(t, conn3.Ping())
}

func Test647_Connector(t *testing.T) {
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
	}
	conn := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn).Ping())
	// reuse options
	conn2 := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn2).Ping())
	// allow nil to be parsed - should work if ClickHouse was available on 9000
	//conn3 := clickhouse.Connector(nil)
	//require.NoError(t, sql.OpenDB(conn3).Ping())
}
