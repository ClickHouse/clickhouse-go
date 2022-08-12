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
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/google/uuid"
)

func Sessions() error {
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
		Settings: clickhouse.Settings{
			"session_id": uuid.NewString(),
		},
	})
	if _, err := conn.Exec(`DROP TABLE IF EXISTS example`); err != nil {
		return err
	}
	_, err := conn.Exec(`
		CREATE TEMPORARY TABLE IF NOT EXISTS example (
			  Col1 UInt8
		)
	`)
	if err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare("INSERT INTO example")
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		_, err := batch.Exec(
			uint8(i),
		)
		if err != nil {
			return err
		}
	}
	rows, err := conn.Query("SELECT * FROM example")
	if err != nil {
		return err
	}
	var (
		col1 uint8
	)
	for rows.Next() {
		if err := rows.Scan(&col1); err != nil {
			return err
		}
		fmt.Printf("row: col1=%\n", col1)
	}

	return nil
}