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
	"database/sql"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func ConnectHTTP() error {
	port := clickhouse_tests.GetEnv("CLICKHOUSE_HTTP_PORT", "8123")
	host := clickhouse_tests.GetEnv("CLICKHOUSE_HOST", "localhost")
	username := clickhouse_tests.GetEnv("CLICKHOUSE_USERNAME", "default")
	password := clickhouse_tests.GetEnv("CLICKHOUSE_PASSWORD", "")
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: username,
			Password: password,
		},
		Protocol: clickhouse.HTTP,
	})
	return conn.Ping()
}

func ConnectDSNHTTP() error {
	port := clickhouse_tests.GetEnv("CLICKHOUSE_HTTP_PORT", "8123")
	host := clickhouse_tests.GetEnv("CLICKHOUSE_HOST", "localhost")
	username := clickhouse_tests.GetEnv("CLICKHOUSE_USERNAME", "default")
	password := clickhouse_tests.GetEnv("CLICKHOUSE_PASSWORD", "")
	conn, err := sql.Open("clickhouse", fmt.Sprintf("http://%s:%s?username=%s&password=%s", host, port, username, password))
	if err != nil {
		return err
	}
	return conn.Ping()
}
