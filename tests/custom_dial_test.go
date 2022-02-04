package tests

import (
	"context"
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestCustomDial(t *testing.T) {
	var (
		dialCount int
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"127.0.0.1:9000"},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
				Password: "",
			},
			Dial: func(addr string, opt *clickhouse.Options) (net.Conn, error) {
				dialCount++
				return net.Dial("tcp", addr)
			},
		})
	)
	if !assert.NoError(t, err) {
		return
	}
	if err := conn.Ping(context.Background()); assert.NoError(t, err) {
		assert.Equal(t, 1, dialCount)
	}
}
