
package std

import (
	"context"
	"crypto/tls"
	"fmt"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdCustomDial(t *testing.T) {
	env, err := GetStdTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	port := env.Port
	var tlsConfig *tls.Config
	if useSSL {
		port = env.SslPort
		tlsConfig = &tls.Config{}
	}
	var (
		dialCount int
		conn      = clickhouse.OpenDB(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: env.Username,
				Password: env.Password,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": 60,
			},
			DialTimeout: 5 * time.Second,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
				dialCount++
				if tlsConfig != nil {
					return tls.DialWithDialer(&net.Dialer{Timeout: time.Duration(5) * time.Second}, "tcp", addr, tlsConfig)
				}
				return net.Dial("tcp", addr)
			},
			TLS: tlsConfig,
		})
	)
	require.NoError(t, conn.Ping())
	assert.Equal(t, 1, dialCount)
}
