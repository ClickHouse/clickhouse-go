package main

import (
	"database/sql/driver"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := clickhouse.Open("tcp://127.0.0.1:9000?username=&debug=true")
	if err != nil {
		log.Fatal(err)
	}
	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare(`
			CREATE TABLE IF NOT EXISTS example (
				country_code FixedString(2),
				os_id        UInt8,
				browser_id   UInt8,
				categories   Array(Int16),
				action_day   Date,
				action_time  DateTime
			) engine=Memory
		`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		tx.Commit()
	}
	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
		for i := 0; i < 100; i++ {
			if _, err := stmt.Exec([]driver.Value{
				"CZ",
				uint8(10 + i),
				uint8(100 + i),
				clickhouse.Array([]int16{1, 2, 3}),
				time.Now(),
				time.Now(),
			}); err != nil {
				log.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare(`DROP TABLE example`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		tx.Commit()
	}
}
