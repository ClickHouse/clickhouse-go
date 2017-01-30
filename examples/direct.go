package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/kshvakov/clickhouse"
)

func query(query string) (driver.Rows, error) {
	tx, _ := connect.Begin()
	stmt, _ := tx.Prepare(query)
	rows, err := stmt.Query([]driver.Value{})
	stmt.Close()
	return rows, err
}

func exec(query string) error {
	tx, _ := connect.Begin()
	stmt, _ := tx.Prepare(query)
	if err := stmt.Exec([]driver.Value{}); err != nil {
		return err
	}
	return tx.Commit()
}

func main() {
	connect, err := clickhouse.Open("tcp://127.0.0.1:9000?username=&debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	if err := exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`); err != nil {
		log.Fatal(err)
	}

	tx, _ = connect.Begin()
	stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec([]driver.Value{
			"CZ",
			10 + i,
			100 + i,
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

	rows, err := query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
