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

package std

import (
	"crypto/tls"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/url"
	"strconv"
	"testing"
)

func TestCompressionStd(t *testing.T) {
	type compressionTest struct {
		compressionMethods []clickhouse.CompressionMethod
	}

	protocols := map[clickhouse.Protocol]compressionTest{clickhouse.HTTP: {
		compressionMethods: []clickhouse.CompressionMethod{clickhouse.CompressionLZ4, clickhouse.CompressionZSTD, clickhouse.CompressionGZIP, clickhouse.CompressionDeflate, clickhouse.CompressionBrotli},
	}, clickhouse.Native: {
		compressionMethods: []clickhouse.CompressionMethod{clickhouse.CompressionLZ4, clickhouse.CompressionZSTD},
	}}

	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	if useSSL {
		tlsConfig = &tls.Config{}
	}
	for protocol, compressionTest := range protocols {
		for _, method := range compressionTest.compressionMethods {
			t.Run(fmt.Sprintf("%s with %s", protocol, method), func(t *testing.T) {
				conn, err := GetStdOpenDBConnection(protocol, clickhouse.Settings{
					"max_execution_time":      60,
					"enable_http_compression": 1, // needed for http compression e.g. gzip
				}, tlsConfig, &clickhouse.Compression{
					Method: method,
					Level:  3,
				})
				require.NoError(t, err)
				conn.Exec("DROP TABLE IF EXISTS test_array_compress")
				const ddl = `
					CREATE TABLE test_array_compress (
						  Col1 Array(Int32),
					      Col2 Int32         
					) Engine MergeTree() ORDER BY tuple()
					`
				defer func() {
					conn.Exec("DROP TABLE test_array_compress")
				}()
				_, err = conn.Exec(ddl)
				require.NoError(t, err)
				scope, err := conn.Begin()
				require.NoError(t, err)
				batch, err := scope.Prepare("INSERT INTO test_array_compress")
				require.NoError(t, err)
				for i := int32(0); i < 100; i++ {
					_, err := batch.Exec([]int32{i, i + 1, i + 2}, i)
					require.NoError(t, err)
				}
				require.NoError(t, scope.Commit())
				rows, err := conn.Query("SELECT * FROM test_array_compress ORDER BY Col2 ASC")
				require.NoError(t, err)
				i := int32(0)
				for rows.Next() {
					var (
						col1 any
						col2 int32
					)
					require.NoError(t, rows.Scan(&col1, &col2))
					assert.Equal(t, i, col2)
					assert.Equal(t, []int32{i, i + 1, i + 2}, col1)
					i += 1
				}
				require.NoError(t, rows.Close())
				require.NoError(t, rows.Err())
				scope, err = conn.Begin()
				require.NoError(t, err)
				batch, err = scope.Prepare("INSERT INTO test_array_compress")
				require.NoError(t, err)
				for i := int32(100); i < 200; i++ {
					_, err := batch.Exec([]int32{i, i + 1, i + 2}, i)
					require.NoError(t, err)
				}
				require.NoError(t, scope.Commit())
				require.NoError(t, err)
				i = 0
				for rows.Next() {
					var (
						col1 any
						col2 int32
					)
					require.NoError(t, rows.Scan(&col1, &col2))
					assert.Equal(t, i, col2)
					assert.Equal(t, []int32{i, i + 1, i + 2}, col1)
					i += 1
				}
			})
		}
	}
}

func TestCompressionStdDSN(t *testing.T) {
	dsns := map[string]clickhouse.Protocol{"Native": clickhouse.Native, "Http": clickhouse.HTTP}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol, useSSL, url.Values{"compress": []string{"true"}})
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS test_array_compress")
			const ddl = `
				CREATE TABLE test_array_compress (
					  Col1 Array(String)
				) Engine MergeTree() ORDER BY tuple()
				`
			defer func() {
				conn.Exec("DROP TABLE test_array_compress")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_array_compress")
			require.NoError(t, err)
			var (
				col1Data = []string{"A", "b", "c"}
			)
			for i := 0; i < 100; i++ {
				_, err := batch.Exec(col1Data)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM test_array_compress")
			require.NoError(t, err)
			for rows.Next() {
				var (
					col1 any
				)
				require.NoError(t, rows.Scan(&col1))
				assert.Equal(t, col1Data, col1)
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}

type protocolCompress struct {
	protocol clickhouse.Protocol
	compress string
	level    string
}

func TestCompressionStdDSNWithLevel(t *testing.T) {
	dsns := map[string]protocolCompress{"Native": {
		protocol: clickhouse.Native,
		compress: "lz4",
	}, "Http": {
		protocol: clickhouse.HTTP,
		compress: "gzip",
		level:    "9",
	}}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, protocol := range dsns {
		t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
			conn, err := GetStdDSNConnection(protocol.protocol, useSSL, nil)
			require.NoError(t, err)
			conn.Exec("DROP TABLE IF EXISTS test_array_compress")
			const ddl = `
				CREATE TABLE test_array_compress (
					  Col1 Array(String)
				) Engine MergeTree() ORDER BY tuple()
				`
			defer func() {
				conn.Exec("DROP TABLE test_array_compress")
			}()
			_, err = conn.Exec(ddl)
			require.NoError(t, err)
			scope, err := conn.Begin()
			require.NoError(t, err)
			batch, err := scope.Prepare("INSERT INTO test_array_compress")
			require.NoError(t, err)
			var (
				col1Data = []string{"A", "b", "c"}
			)
			for i := 0; i < 100; i++ {
				_, err := batch.Exec(col1Data)
				require.NoError(t, err)
			}
			require.NoError(t, scope.Commit())
			rows, err := conn.Query("SELECT * FROM test_array_compress")
			require.NoError(t, err)
			for rows.Next() {
				var (
					col1 any
				)
				require.NoError(t, rows.Scan(&col1))
				assert.Equal(t, col1Data, col1)
			}
			require.NoError(t, rows.Close())
			require.NoError(t, rows.Err())
		})
	}
}

func TestCompressionStdDSNInvalid(t *testing.T) {
	// these should all fail
	configs := map[string][]protocolCompress{"Native": {{
		protocol: clickhouse.Native,
		compress: "gzip",
	}}, "Http": {{
		protocol: clickhouse.HTTP,
		compress: "gzip",
		level:    "10",
	}, {
		protocol: clickhouse.HTTP,
		compress: "gzip",
		level:    "-3",
	}}}
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	for name, dsns := range configs {
		for _, dsn := range dsns {
			t.Run(fmt.Sprintf("%s Protocol", name), func(t *testing.T) {
				conn, err := GetStdDSNConnection(dsn.protocol, useSSL, url.Values{
					"compress":       []string{dsn.compress},
					"compress_level": []string{dsn.level},
				})
				const ddl = `
				CREATE TABLE test_array_compress (
					  Col1 Array(String)
				) Engine MergeTree() ORDER BY tuple()
				`
				_, err = conn.Exec(ddl)
				require.Error(t, err)
			})
		}
	}
}
