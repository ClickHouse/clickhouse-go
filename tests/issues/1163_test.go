package issues

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1163(t *testing.T) {
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
	var debugfCalled bool
	options := &clickhouse.Options{
		Addr:  []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Debug: true,
		Debugf: func(format string, v ...any) {
			debugfCalled = true
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: env.Username,
			Password: env.Password,
		},
		TLS: tlsConfig,
	}
	conn := clickhouse.Connector(options)
	c, err := conn.Connect(context.TODO())
	require.NoError(t, err)
	require.NotNil(t, c)
	assert.True(t, debugfCalled)
}
