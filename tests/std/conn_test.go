package std

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if assert.NoError(t, err) {
			if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
				if assert.NoError(t, conn.Close()) {
					t.Log(conn.Stats())
				}
			}
		}
	}
}
func TestConnFailover(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9000"); assert.NoError(t, err) {
		if err := conn.PingContext(context.Background()); assert.NoError(t, err) {
			t.Log(conn.PingContext(context.Background()))
		}
	}
}
func TestPingDeadline(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		if err := conn.PingContext(ctx); assert.Error(t, err) {
			assert.Equal(t, err, context.DeadlineExceeded)
		}
	}
}
