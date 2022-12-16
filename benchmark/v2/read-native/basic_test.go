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

package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"testing"
	"time"
)

func getConnection() clickhouse.Conn {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		//Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		BlockBufferSize: 100,
	})
	if err != nil {
		log.Fatal(err)
	}
	return conn
}

func BenchmarkRead(b *testing.B) {
	b.Run("string", benchmarkStringRead)
	b.Run("random", benchmarkRandom)
}

func benchmarkRandom(b *testing.B) {
	conn := getConnection()
	b.ResetTimer()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT number, randomString(25), array(1, 2, 3, 4, 5), now() FROM system.numbers LIMIT %d`, b.N))
	if err != nil {
		b.Fatal(err)
	}
	i := 0
	for rows.Next() {
		var (
			col1 uint64
			col2 string
			col3 []uint8
			col4 time.Time
		)
		if err := rows.Scan(&col1, &col2, &col3, &col4); err != nil {
			b.Fatal(err)
		}
		i++
		if i == b.N {
			break
		}
	}
}

func benchmarkStringRead(b *testing.B) {
	conn := getConnection()
	b.ResetTimer()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT toString(number) FROM numbers(%d)`, b.N))
	if err != nil {
		b.Fatal(err)
	}
	i := 0
	for rows.Next() {
		var (
			col1 string
		)
		if err := rows.Scan(&col1); err != nil {
			b.Fatal(err)
		}
		i++
		if i == b.N {
			break
		}
	}
}
