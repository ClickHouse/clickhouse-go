package main

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/lib/column"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=&compress=true&debug=true")
	checkErr(err)
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	_, err = connect.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)

	checkErr(err)
	tx, err := connect.Begin()
	checkErr(err)
	stmt, err := tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	checkErr(err)

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			[]int16{1, 2, 3},
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}
	checkErr(tx.Commit())

	col, err := column.Factory("country_code", "String", nil)
	checkErr(err)
	countriesExternalTable := clickhouse.ExternalTable{
		Name: "countries",
		Values: [][]driver.Value{
			{"RU"},
		},
		Columns: []column.Column{col},
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time "+
		"FROM example WHERE country_code IN ?", countriesExternalTable)
	checkErr(err)
	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		checkErr(rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime))
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
