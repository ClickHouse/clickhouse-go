package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func example() error {
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
	})
	if err != nil {
		return err
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS example`); err != nil {
		return err
	}
	err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS example (
			Col1 UInt8,
			Col2 String,
			Col3 DateTime
		) engine=Memory
	`)
	if err != nil {
		return err
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example (Col1, Col2, Col3)")
	if err != nil {
		return err
	}
	for i := 0; i < 10; i++ {
		if err := batch.Append(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return err
	}

	var result []struct {
		Col1           uint8
		Col2           string
		ColumnWithName time.Time `ch:"Col3"`
	}

	if err = conn.Select(ctx, &result, "SELECT Col1, Col2, Col3 FROM example"); err != nil {
		return err
	}

	for _, v := range result {
		fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", v.Col1, v.Col2, v.ColumnWithName)
	}

	return nil
}

func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
