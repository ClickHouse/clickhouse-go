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
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		//Debug: true,
	})
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal(err)
	}
	var settings []struct {
		Name        string  `ch:"name"`
		Value       string  `ch:"value"`
		Changed     uint8   `ch:"changed"`
		Description string  `ch:"description"`
		Min         *string `ch:"min"`
		Max         *string `ch:"max"`
		Readonly    uint8   `ch:"readonly"`
		Type        string  `ch:"type"`
	}
	if err = conn.Select(context.Background(), &settings, "SELECT * FROM system.settings WHERE name LIKE $1 ORDER BY length(name) LIMIT 5", "%max%"); err != nil {
		log.Fatal(err)
	}
	for _, s := range settings {
		fmt.Printf("name: %s, value: %s, type=%s\n", s.Name, s.Value, s.Type)
	}

	if err = conn.Exec(context.Background(), "TUNCATE TABLE X"); err == nil {
		panic("unexpected")
	}
	if exception, ok := err.(*clickhouse.Exception); ok {
		fmt.Printf("Catch exception [%d]\n", exception.Code)
	}
	const ddl = `
	CREATE TABLE example (
		  Col1 UInt64
		, Col2 FixedString(2)
		, Col3 Map(String, String)
		, Col4 Array(String)
		, Col5 DateTime64(3)
	) Engine Memory
	`
	if err := conn.Exec(context.Background(), "DROP TABLE IF EXISTS example"); err != nil {
		log.Fatal(err)
	}
	if err := conn.Exec(context.Background(), ddl); err != nil {
		log.Fatal(err)
	}
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO example")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10_000; i++ {
		err := batch.Append(
			uint64(i),
			"CH",
			map[string]string{
				"key": "value",
			},
			[]string{"A", "B", "C"},
			time.Now(),
		)
		if err != nil {
			log.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		log.Fatal(err)
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))

	var count uint64
	if err := conn.QueryRow(ctx, "SELECT COUNT() FROM example").Scan(&count); err != nil {
		log.Fatal(err)
	}
	fmt.Println("count", count)
	var result struct {
		Col1  uint64
		Count uint64 `ch:"count"`
	}
	if err := conn.QueryRow(ctx, "SELECT Col1, COUNT() AS count FROM example WHERE Col1 = $1 GROUP BY Col1", 42).ScanStruct(&result); err != nil {
		log.Fatal(err)
	}
	fmt.Println("result", result)
}
