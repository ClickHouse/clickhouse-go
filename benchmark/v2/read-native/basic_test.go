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
