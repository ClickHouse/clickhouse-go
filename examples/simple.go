package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

func main() {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123?compress=true&debug=true")
	if err != nil {
		log.Fatal(err)
	}
	_, err = connect.Exec(`
        CREATE TABLE example (
            country_code FixedString(2),
            os_id        UInt8,
            browser_id   UInt8,
            action_time  DateTime
        ) engine=Memory
    `)

	if err != nil {
		log.Fatal(err)
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, action_time) VALUES (?, ?, ?, ?)")
	)

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec("RU", 100+i, 200+i, time.Now()); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country     string
			os, browser uint8
			actionTime  time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, action_time: %s", country, os, browser, actionTime)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
