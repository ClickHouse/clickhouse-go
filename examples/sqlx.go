package main

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

func main() {
	connect, err := sqlx.Open("clickhouse", "http://127.0.0.1:8123?compress=true&debug=true")
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
		if _, err := stmt.Exec("RU", 10+i, 100+i, time.Now()); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	var items []struct {
		CountryCode string    `db:"country_code"`
		OsID        uint8     `db:"os_id"`
		BrowserID   uint8     `db:"browser_id"`
		ActionTime  time.Time `db:"action_time"`
	}

	if err := connect.Select(&items, "SELECT country_code, os_id, browser_id, action_time FROM example"); err != nil {
		log.Fatal(err)
	}

	for _, item := range items {
		log.Printf("country: %s, os: %d, browser: %d, action_time: %s", item.CountryCode, item.OsID, item.BrowserID, item.ActionTime)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
