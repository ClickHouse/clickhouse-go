package issues

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test570(t *testing.T) {

	// using ParseDNS - defaults shouldn't be set for maxOpenConnections etc
	options, err := clickhouse.ParseDSN("clickhouse://default:@127.0.0.1:9000/default")
	assert.NoError(t, err)
	conn := clickhouse.OpenDB(options)
	conn.SetMaxOpenConns(5)
	conn.SetMaxIdleConns(10)
	assert.NoError(t, conn.Ping())
	conn.Close()

	// check we can pass Options
	options = &clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}
	conn = clickhouse.OpenDB(options)
	assert.NoError(t, conn.Ping())

	// check we can open with a DSN
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		assert.NoError(t, conn.Ping())
	}
}
