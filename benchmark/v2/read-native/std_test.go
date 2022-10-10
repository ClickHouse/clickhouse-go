package main

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"testing"
	"time"
)

func _getConnection() *sql.DB {
	conn := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:8123"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 10 * time.Second,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Protocol:        clickhouse.HTTP,
		BlockBufferSize: 100,
	})
	return conn
}
func TestStdRead(b *testing.T) {
	db := _getConnection()
	start := time.Now()
	rows, err := db.Query("SELECT number FROM system.numbers_mt LIMIT 500000000")
	if err != nil {
		b.Fatal(err)
	}
	var (
		col1 uint64
	)
	for rows.Next() {
		if err := rows.Scan(&col1); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := time.Since(start)
	log.Printf("Read took %s", elapsed)
}
