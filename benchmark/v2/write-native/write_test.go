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
			Password: "ClickHouse",
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

func BenchmarkWrite(b *testing.B) {
	b.Run("simple", benchmarkSimpleWrite)
}

func benchmarkSimpleWrite(b *testing.B) {
	conn := getConnection()

	if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS benchmark"); err != nil {
		b.Fatal(err)
	}
	const ddl = `
		CREATE TABLE benchmark (
			  Col1 UInt64
			, Col2 String
			, Col3 Array(UInt8)
			, Col4 DateTime
		) Engine Null
		`

	if err := conn.Exec(context.Background(), ddl); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO benchmark")
		if err != nil {
			b.Fatal(err)
		}
		for c := 0; c < 10_000_000; c++ {
			err := batch.Append(
				uint64(i),
				"Golang SQL database driver",
				[]uint8{1, 2, 3, 4, 5, 6, 7, 8, 9},
				time.Now(),
			)
			if err != nil {
				b.Fatal(err)
			}
		}

		if err := batch.Send(); err != nil {
			b.Fatal(err)
		}
	}
}
