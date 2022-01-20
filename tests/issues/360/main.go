package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var conn *sql.DB

func main() {
	go func() {
		http.ListenAndServe("127.0.0.1:9876", nil)
	}()

	var err error
	conn, err = sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=false")
	if err != nil {
		log.Fatal(err)
	}
	conn.SetMaxOpenConns(5)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(15 * time.Minute)
	if err := conn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}
	conn.Exec("DROP TABLE IF EXISTS example")
	_, err = conn.Exec(`
		CREATE TABLE IF NOT EXISTS example (
			country_code FixedString(2),
			os_id        UInt8,
			browser_id   UInt8,
			categories   Array(Int16),
			action_day   Date,
			action_time  DateTime
		) engine=Memory
	`)

	if err != nil {
		log.Fatal(err)
	}

	for range time.Tick(time.Second) {
		log.Println("time", time.Now())
		//go testInsert()
		//go testQuery()
		testQuery()
	}

	if _, err := conn.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}

func testInsert() {
	var (
		tx, _   = conn.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, action_day, action_time) VALUES (?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func testQuery() {
	rows, err := conn.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var (
		country               string
		os, browser           uint8
		categories            []int16
		actionDay, actionTime time.Time
	)
	for rows.Next() {
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		//log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
