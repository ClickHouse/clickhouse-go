package main

import (
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"time"
)

func example() error {
	conn, err := sql.Open("clickhousehttp", "http://127.0.0.1:8123?dial_timeout=1s&compress=true")
	if err != nil {
		return err
	}

	if _, err := conn.Exec(`DROP TABLE IF EXISTS example3`); err != nil {
		return err
	}
	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS example3 (
			Col1 UInt8
			,Col2 String
			,Col3 Int32	
		) Engine = Memory
	`)
	if err != nil {
		return err
	}
	scope, err := conn.Begin()
	if err != nil {
		return err
	}
	batch, err := scope.Prepare("INSERT INTO example3 VALUES ")
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		_, err := batch.Exec(
			//context.Background(),
			uint8(42),
			"ClickHouse",
			34,
		)
		if err != nil {
			return err
		}
	}
	return scope.Commit()
}

func main() {
	start := time.Now()
	if err := example(); err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}
