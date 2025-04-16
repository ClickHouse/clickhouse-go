// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package clickhouse

import (
	"crypto/tls"
	"net/url"
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
