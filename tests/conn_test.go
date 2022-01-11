package tests

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			if assert.NoError(t, conn.Close()) {
				t.Log(conn.Stats())
				t.Log(conn.ServerVersion())
				t.Log(conn.Ping(context.Background()))
			}
		}
	}
}
func TestConnFailover(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			"127.0.0.1:9001",
			"127.0.0.1:9002",
			"127.0.0.1:9000",
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//	Debug: true,
	})
	if assert.NoError(t, err) {
		if err := conn.Ping(context.Background()); assert.NoError(t, err) {
			t.Log(conn.ServerVersion())
			t.Log(conn.Ping(context.Background()))
		}
	}
}
func TestPingDeadline(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		//Debug: true,
	})
	if assert.NoError(t, err) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if err := conn.Ping(ctx); assert.Error(t, err) {
			assert.Equal(t, err, context.DeadlineExceeded)
		}
	}
}
