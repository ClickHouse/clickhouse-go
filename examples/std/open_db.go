package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func example() error {
	conn := sql.OpenDB(clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	}))
	conn.SetMaxIdleConns(5)
	conn.SetMaxOpenConns(10)
	conn.SetConnMaxLifetime(time.Hour)
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
		"max_block_size": 10,
	}), clickhouse.WithProgress(func(p *clickhouse.Progress) {
		fmt.Println("progress: ", p)
	}))
	if err := conn.PingContext(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return err
	}
	if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS example`); err != nil {
		return err
	}
	_, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS example (
			Col1 UInt8,
			Col2 String,
			Col3 DateTime
		) engine=Memory
	`)
	if err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	{
		batch, err := scope.PrepareContext(ctx, "INSERT INTO example (Col1, Col2, Col3)")
		if err != nil {
			return err
		}
		for i := 0; i < 10; i++ {
			if _, err := batch.Exec(uint8(i), fmt.Sprintf("value_%d", i), time.Now()); err != nil {
				return err
			}
		}
	}
	if err := scope.Commit(); err != nil {
		return err
	}
	rows, err := conn.QueryContext(ctx, "SELECT Col1, Col2, Col3 FROM example WHERE Col1 >= $1 AND Col2 <> $2 AND Col3 <= $3", 0, "xxx", time.Now())
	if err != nil {
		return err
	}
	for rows.Next() {
		var (
			col1 uint8
			col2 string
			col3 time.Time
		)
		if err := rows.Scan(&col1, &col2, &col3); err != nil {
			return err
		}
		fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", col1, col2, col3)
	}
	rows.Close()
	return rows.Err()
}

func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
