package issues

import (
	"context"
	"crypto/x509"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

// TestIssue1300 locks end-to-end behavior of the `tls_server_name` DSN option.
//
// The bundled test certificate (tests/resources/clickhouse.crt) has
// `SAN IP:127.0.0.1` and no DNS SAN. Connecting by IP with full verification
// succeeds only when ServerName is unset. Go TLS then uses the connection
// host (127.0.0.1), which matches the IP SAN.
//
// Setting ServerName to a different DNS name must fail hostname verification. If a future refactor
// silently drops the ServerName field between DSN parsing and the TLS
// handshake, the negative case starts succeeding and this test breaks.
func TestIssue1300(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	if !useSSL {
		t.Skip("CLICKHOUSE_USE_SSL=false; skipping TLS ServerName regression test")
	}
	env, err := GetIssuesTestEnvironment()
	require.NoError(t, err)

	// Build a trust pool that works in both CI lanes:
	//   - docker (self-signed cert): needs CAroot.crt from tests/resources
	//   - cloud (publicly-signed cert): needs the system trust store
	caPool, err := x509.SystemCertPool()
	if err != nil || caPool == nil {
		caPool = x509.NewCertPool()
	}
	cwd, err := os.Getwd()
	require.NoError(t, err)
	if caPEM, err := os.ReadFile(path.Join(cwd, "../resources/CAroot.crt")); err == nil {
		caPool.AppendCertsFromPEM(caPEM)
	}

	baseDSN := fmt.Sprintf("clickhouse://%s:%s@%s:%d/default?secure=true",
		env.Username, env.Password, env.Host, env.SslPort)

	t.Run("no tls_server_name connects against IP SAN", func(t *testing.T) {
		opts, err := clickhouse.ParseDSN(baseDSN)
		require.NoError(t, err)
		require.NotNil(t, opts.TLS)
		require.Empty(t, opts.TLS.ServerName)
		opts.TLS.RootCAs = caPool

		conn, err := clickhouse.Open(opts)
		require.NoError(t, err)
		defer conn.Close()
		require.NoError(t, conn.Ping(context.Background()))
	})

	t.Run("wrong tls_server_name fails hostname verification", func(t *testing.T) {
		dsn := baseDSN + "&tls_server_name=wrong.example.com"
		opts, err := clickhouse.ParseDSN(dsn)
		require.NoError(t, err)
		require.NotNil(t, opts.TLS)
		require.Equal(t, "wrong.example.com", opts.TLS.ServerName)
		opts.TLS.RootCAs = caPool

		conn, err := clickhouse.Open(opts)
		if err == nil {
			defer conn.Close()
			err = conn.Ping(context.Background())
		}
		require.Error(t, err)
		require.Contains(t, strings.ToLower(err.Error()), "x509",
			"expected x509 TLS verification error, got: %v", err)
	})
}
