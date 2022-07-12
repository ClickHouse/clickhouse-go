package issues

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

func Test647(t *testing.T) {
	options := &clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	}
	conn, err := clickhouse.Open(options)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, conn.Ping(ctx))
	//reuse options
	conn2, err := clickhouse.Open(options)
	require.NoError(t, err)
	require.NoError(t, conn2.Ping(ctx))
	conn3, err := clickhouse.Open(nil)
	require.NoError(t, err)
	require.NoError(t, conn3.Ping(ctx))
}

func Test647_OpenDB(t *testing.T) {
	options := &clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	}
	conn := clickhouse.OpenDB(options)
	require.NoError(t, conn.Ping())
	//reuse options
	conn2 := clickhouse.OpenDB(options)
	require.NoError(t, conn2.Ping())
	// allow nil to be parsed
	conn3 := clickhouse.OpenDB(nil)
	require.NoError(t, conn3.Ping())
}

func Test647_Connector(t *testing.T) {
	options := &clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	}
	conn := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn).Ping())
	// reuse options
	conn2 := clickhouse.Connector(options)
	require.NoError(t, sql.OpenDB(conn2).Ping())
	// allow nil to be parsed
	conn3 := clickhouse.Connector(nil)
	require.NoError(t, sql.OpenDB(conn3).Ping())
}
