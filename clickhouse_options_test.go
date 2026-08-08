package clickhouse

import (
	"crypto/tls"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseDSN does not implement all use cases yet
func TestParseDSN(t *testing.T) {
	testCases := []struct {
		name        string
		dsn         string
		expected    *Options
		expectedErr string
	}{
		{
			"empty dsn",
			"",
			nil,
			"parse dsn address failed",
		},
		{
			"no host",
			"/test_database",
			nil,
			"parse dsn address failed",
		},
		{
			"no protocol",
			"127.0.0.1/test_database",
			nil,
			"parse dsn address failed",
		},
		{
			"native protocol",
			"clickhouse://127.0.0.1/",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				scheme:   "clickhouse",
			},
			"",
		},
		{
			"http protocol",
			"http://127.0.0.1/",
			&Options{
				Protocol: HTTP,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				scheme:   "http",
			},
			"",
		},
		{
			"native protocol",
			"clickhouse://127.0.0.1/test_database",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"http protocol",
			"http://127.0.0.1/test_database",
			&Options{
				Protocol: HTTP,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "http",
			},
			"",
		},
		{
			"native protocol with user",
			"clickhouse://user@127.0.0.1/test_database",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
					Username: "user",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with authenticated user",
			"clickhouse://joe:Ys31@127.0.0.1/test_database",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
					Username: "joe",
					Password: "Ys31",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with debug",
			"clickhouse://127.0.0.1/test_database?debug=true",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				Debug:  true,
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with secure",
			"clickhouse://127.0.0.1/test_database?secure=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with skip_verify",
			"clickhouse://127.0.0.1/test_database?secure=true&skip_verify=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: true,
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with secure (legacy)",
			"clickhouse://127.0.0.1/test_database?secure",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with skip_verify (legacy)",
			"clickhouse://127.0.0.1/test_database?secure&skip_verify",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: true,
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with secure and no tls_server_name",
			"clickhouse://127.0.0.1/test_database?secure=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
					ServerName:         "",
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with tls_server_name",
			"clickhouse://127.0.0.1/test_database?secure=true&tls_server_name=clickhouse.local",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
					ServerName:         "clickhouse.local",
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with tls_server_name (empty)",
			"clickhouse://127.0.0.1/test_database?secure=true&tls_server_name=",
			nil,
			"clickhouse [dsn parse]: tls_server_name must not be empty",
		},
		{
			"native protocol with tls_server_name without secure",
			"clickhouse://127.0.0.1/test_database?tls_server_name=clickhouse.local",
			nil,
			"clickhouse [dsn parse]: tls_server_name requires secure=true",
		},
		{
			"native protocol with secure (bad)",
			"clickhouse://127.0.0.1/test_database?secure=ture",
			nil,
			"clickhouse [dsn parse]:secure: strconv.ParseBool: parsing \"ture\": invalid syntax",
		},
		{
			"native protocol with skip_verify (bad)",
			"clickhouse://127.0.0.1/test_database?secure&skip_verify=ture",
			nil,
			"clickhouse [dsn parse]:verify: strconv.ParseBool: parsing \"ture\": invalid syntax",
		},
		{
			"native protocol with default lz4 compression",
			"clickhouse://127.0.0.1/test_database?compress=true",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionLZ4,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with none compression",
			"clickhouse://127.0.0.1/test_database?compress=none",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionNone,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with zstd compression",
			"clickhouse://127.0.0.1/test_database?compress=zstd",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionZSTD,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with lz4 compression",
			"clickhouse://127.0.0.1/test_database?compress=lz4",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionLZ4,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with gzip compression",
			"clickhouse://127.0.0.1/test_database?compress=gzip",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionGZIP,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with deflate compression",
			"clickhouse://127.0.0.1/test_database?compress=deflate",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionDeflate,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with br compression",
			"clickhouse://127.0.0.1/test_database?compress=br",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionBrotli,
					Level:  3,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with default lz4 compression and compression level 5",
			"clickhouse://127.0.0.1/test_database?compress=true&compress_level=5",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Compression: &Compression{
					Method: CompressionLZ4,
					Level:  5,
				},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with 1KiB max compression buffer",
			"clickhouse://127.0.0.1/test_database?max_compression_buffer=1024",
			&Options{
				Protocol:             Native,
				TLS:                  nil,
				Addr:                 []string{"127.0.0.1"},
				Settings:             Settings{},
				MaxCompressionBuffer: 1024,
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"native protocol with invalid numeric max compression buffer",
			"clickhouse://127.0.0.1/test_database?max_compression_buffer=onebyte",
			nil,
			"max_compression_buffer invalid value: strconv.Atoi: parsing \"onebyte\": invalid syntax",
		},
		{
			"native protocol with invalid numeric compress level",
			"clickhouse://127.0.0.1/test_database?compress_level=first",
			nil,
			"compress_level invalid value: strconv.ParseInt: parsing \"first\": invalid syntax",
		},
		{
			"native protocol dial timeout",
			"clickhouse://127.0.0.1/test_database?max_compression_buffer=1024",
			&Options{
				Protocol:             Native,
				TLS:                  nil,
				Addr:                 []string{"127.0.0.1"},
				Settings:             Settings{},
				MaxCompressionBuffer: 1024,
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"client info",
			"clickhouse://127.0.0.1/test_database?client_info_product=grafana/6.1,clickhouse-datasource/1.1",
			&Options{
				Protocol: Native,
				ClientInfo: ClientInfo{
					Products: []struct{ Name, Version string }{
						{"grafana", "6.1"},
						{"clickhouse-datasource", "1.1"},
					},
				},
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"client connection pool settings",
			"clickhouse://127.0.0.1/test_database?max_open_conns=-1&max_idle_conns=0&conn_max_lifetime=1h",
			&Options{
				Protocol:        Native,
				MaxOpenConns:    -1,
				MaxIdleConns:    0,
				ConnMaxLifetime: time.Hour,
				Addr:            []string{"127.0.0.1"},
				Settings:        Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"http protocol with proxy",
			"http://127.0.0.1/?http_proxy=http%3A%2F%2Fproxy.example.com%3A3128",
			&Options{
				Protocol:     HTTP,
				TLS:          nil,
				Addr:         []string{"127.0.0.1"},
				Settings:     Settings{},
				scheme:       "http",
				HTTPProxyURL: parseURL(t, "http://proxy.example.com:3128"),
			},
			"",
		},
		{
			"clickhouse proxy with database as query string",
			"tcp://127.0.0.1/?database=bla",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: `bla`,
				},
				scheme: "tcp",
			},
			"",
		},
		{
			"http protocol with custom http_path",
			"https://127.0.0.1/clickhouse?secure=true&skip_verify=true&http_path=/clickhouse",
			&Options{
				Protocol: HTTP,
				TLS: &tls.Config{
					InsecureSkipVerify: true,
				},
				Addr:     []string{"127.0.0.1"},
				Settings: Settings{},
				Auth: Auth{
					Database: "clickhouse",
				},
				HttpUrlPath: "/clickhouse",
				scheme:      "https",
			},
			"",
		},
		{
			"multiple hosts in HA mode",
			"clickhouse://127.0.0.1:9440,127.0.0.2:9440/test_database",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"127.0.0.1:9440", "127.0.0.2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host with auth and secure",
			"clickhouse://user:pass@host1:9440,host2:9440/database?secure=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
				},
				Addr:     []string{"host1:9440", "host2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Username: "user",
					Password: "pass",
					Database: "database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host HTTP scheme",
			"http://host1:8123,host2:8123/db",
			&Options{
				Protocol: HTTP,
				TLS:      nil,
				Addr:     []string{"host1:8123", "host2:8123"},
				Settings: Settings{},
				Auth: Auth{
					Database: "db",
				},
				scheme: "http",
			},
			"",
		},
		{
			"multi-host IPv6",
			"clickhouse://[::1]:9440,[2001:db8::1]:9440/test_database",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"[::1]:9440", "[2001:db8::1]:9440"},
				Settings: Settings{},
				Auth: Auth{
					Database: "test_database",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host with auth overridden via query params",
			"clickhouse://user:pass@host1:9440,host2:9440/db?username=other&password=secret",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"host1:9440", "host2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Username: "other",
					Password: "secret",
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host with auth via query only no userinfo",
			"clickhouse://host1:9440,host2:9440/db?username=quser&password=qpass",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"host1:9440", "host2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Username: "quser",
					Password: "qpass",
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host with encoded password and secure",
			"clickhouse://user:p%40ss%3Aw0rd@host1:9440,host2:9440/db?secure=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
				},
				Addr:     []string{"host1:9440", "host2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Username: "user",
					Password: "p@ss:w0rd",
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host IPv6 with auth and database",
			"clickhouse://ipv6user:ipv6pass@[::1]:9440,[2001:db8::1]:9440/test_db?secure=true",
			&Options{
				Protocol: Native,
				TLS: &tls.Config{
					InsecureSkipVerify: false,
				},
				Addr:     []string{"[::1]:9440", "[2001:db8::1]:9440"},
				Settings: Settings{},
				Auth: Auth{
					Username: "ipv6user",
					Password: "ipv6pass",
					Database: "test_db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host preserves hosts despite userinfo containing colon and at",
			"clickhouse://user:pass@host1:9000,host2:9000,host3:9000/db",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"host1:9000", "host2:9000", "host3:9000"},
				Settings: Settings{},
				Auth: Auth{
					Username: "user",
					Password: "pass",
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host mixed IPv6 then plain",
			"clickhouse://[::1]:9440,host2:9440/db",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"[::1]:9440", "host2:9440"},
				Settings: Settings{},
				Auth: Auth{
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host mixed plain then IPv6",
			"clickhouse://host1:9440,[::1]:9440/db",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"host1:9440", "[::1]:9440"},
				Settings: Settings{},
				Auth: Auth{
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"IPv6 zone ID percent-decoded in Addr",
			"clickhouse://[fe80::1%25eth0]:9000/db",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"[fe80::1%eth0]:9000"},
				Settings: Settings{},
				Auth: Auth{
					Database: "db",
				},
				scheme: "clickhouse",
			},
			"",
		},
		{
			"multi-host IPv6 with zone ID and auth",
			"clickhouse://zuser:zpass@[fe80::1%25eth0]:9000,[::1]:9000/zdb",
			&Options{
				Protocol: Native,
				TLS:      nil,
				Addr:     []string{"[fe80::1%eth0]:9000", "[::1]:9000"},
				Settings: Settings{},
				Auth: Auth{
					Username: "zuser",
					Password: "zpass",
					Database: "zdb",
				},
				scheme: "clickhouse",
			},
			"",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			opts, err := ParseDSN(testCase.dsn)

			if testCase.expectedErr != "" {
				assert.Nil(t, opts)
				assert.EqualError(t, err, testCase.expectedErr)
				return
			}

			assert.Equal(t, testCase.expected, opts)
			assert.Nil(t, err)
		})
	}
}

func parseURL(t *testing.T, v string) *url.URL {
	u, err := url.Parse(v)
	require.NoError(t, err)
	return u
}

func TestSplitHostList(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"a:1", "b:2"}, splitHostList("a:1,b:2"))
	assert.Equal(t, []string{"[::1]:1", "[::2]:2"}, splitHostList("[::1]:1,[::2]:2"))
	assert.Equal(t, []string{"single"}, splitHostList("single"))
	assert.Nil(t, splitHostList(""))
	// Auth/userinfo should not leak into host list; hosts remain cluster-wide
	assert.Equal(t, []string{"host1:9440", "host2:9440"}, splitHostList("host1:9440,host2:9440"))
	// Mixed bracketed and plain hosts
	assert.Equal(t, []string{"[::1]:9440", "host2:9440"}, splitHostList("[::1]:9440,host2:9440"))
	assert.Equal(t, []string{"host1:9440", "[::1]:9440"}, splitHostList("host1:9440,[::1]:9440"))
	// Zone ID (already unescaped by churl) is preserved through SplitHostPort
	assert.Equal(t, []string{"[fe80::1%eth0]:9000"}, splitHostList("[fe80::1%eth0]:9000"))
	// Bare hosts without ports
	assert.Equal(t, []string{"host1", "host2"}, splitHostList("host1,host2"))
}

// Multi-host DSNs share one Auth for every address. Per-host credentials are not
// supported; query params and post-parse Options overrides remain cluster-wide.
func TestMultiHostAuthClusterWide(t *testing.T) {
	t.Parallel()

	opts, err := ParseDSN("clickhouse://user:pass@host1:9440,host2:9440/db")
	require.NoError(t, err)
	require.Equal(t, []string{"host1:9440", "host2:9440"}, opts.Addr)
	require.Equal(t, Auth{Username: "user", Password: "pass", Database: "db"}, opts.Auth)

	// Query params override userinfo for the whole cluster (both hosts).
	opts, err = ParseDSN("clickhouse://user:pass@host1:9440,host2:9440/db?username=other&password=secret")
	require.NoError(t, err)
	require.Equal(t, []string{"host1:9440", "host2:9440"}, opts.Addr)
	require.Equal(t, Auth{Username: "other", Password: "secret", Database: "db"}, opts.Auth)

	// Callers may replace Auth on Options after ParseDSN; that still applies to all hosts.
	opts.Auth.Username = "override"
	opts.Auth.Password = "newpass"
	require.Equal(t, []string{"host1:9440", "host2:9440"}, opts.Addr)
	require.Equal(t, Auth{Username: "override", Password: "newpass", Database: "db"}, opts.Auth)
}

func TestLogger(t *testing.T) {
	t.Run("debug=1 via DSN produces non-noop logger", func(t *testing.T) {
		opts, err := ParseDSN("clickhouse://127.0.0.1/test?debug=1")
		require.NoError(t, err)
		require.True(t, opts.Debug)

		logger := opts.logger()
		require.NotNil(t, logger)
		_, isNoop := logger.Handler().(*noopHandler)
		assert.False(t, isNoop, "expected non-noop logger when debug=1")
	})

	t.Run("no debug flag produces noop logger", func(t *testing.T) {
		opts, err := ParseDSN("clickhouse://127.0.0.1/test")
		require.NoError(t, err)
		require.False(t, opts.Debug)

		logger := opts.logger()
		require.NotNil(t, logger)
		_, isNoop := logger.Handler().(*noopHandler)
		assert.True(t, isNoop, "expected noop logger when debug is not set")
	})

	t.Run("custom Logger takes precedence over Debug=true", func(t *testing.T) {
		customLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
		opts := &Options{Debug: true, Logger: customLogger}
		logger := opts.logger()
		assert.Equal(t, customLogger, logger, "custom Logger should take precedence over Debug=true when Debugf is nil")
	})
}
