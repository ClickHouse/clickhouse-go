package main

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/stretchr/testify/require"
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

func TestColumnStringRead(b *testing.T) {
	conn := getConnection()
	start := time.Now()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT toString(number) FROM system.numbers_mt LIMIT 500000000`))
	if err != nil {
		b.Fatal(err)
	}
	var (
		val string
	)
	c := 0
	for {
		block := rows.NextBlock()
		if block == nil {
			break
		}
		col := block.Columns[0].(*column.String)
		for i := 0; i < col.Rows(); i++ {
			col.Scan(&val, i)
			c++
		}
	}
	require.Equal(b, 500000000, c)
	elapsed := time.Since(start)
	fmt.Printf("Read took %s", elapsed)
}

func TestRowStringRead(b *testing.T) {
	conn := getConnection()
	start := time.Now()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT toString(number) FROM system.numbers_mt LIMIT 500000000`))
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
	}
	require.Equal(b, 500000000, i)
	elapsed := time.Since(start)
	fmt.Printf("Read took %s", elapsed)
}

func TestRowNumberRead(b *testing.T) {
	conn := getConnection()
	start := time.Now()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT number FROM system.numbers_mt LIMIT 500000000`))
	if err != nil {
		b.Fatal(err)
	}
	i := 0
	for rows.Next() {
		var (
			col1 uint64
		)
		if err := rows.Scan(&col1); err != nil {
			b.Fatal(err)
		}
		i++
	}
	require.Equal(b, 500000000, i)
	elapsed := time.Since(start)
	fmt.Printf("Read took %s", elapsed)
}

func TestColumnNumberRead(b *testing.T) {
	conn := getConnection()
	start := time.Now()
	rows, err := conn.Query(context.Background(), fmt.Sprintf(`SELECT number FROM system.numbers_mt LIMIT 500000000`))
	if err != nil {
		b.Fatal(err)
	}
	var (
		val uint64
	)
	c := 0
	for {
		block := rows.NextBlock()
		if block == nil {
			break
		}
		col := block.Columns[0].(*column.UInt64)
		for i := 0; i < col.Rows(); i++ {
			col.Scan(&val, i)
			c++
		}
	}
	require.Equal(b, 500000000, c)
	elapsed := time.Since(start)
	fmt.Printf("Read took %s", elapsed)
}
