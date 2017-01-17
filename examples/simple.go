package main

import (
	"database/sql"
	"log"
	"time"

	"fmt"

	"github.com/kshvakov/clickhouse"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=&compress=true&debug=true")
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
	/*return
		_, err = connect.Exec(`
	        CREATE TABLE example (
	            country_code FixedString(2),
	            os_id        UInt8,
	            browser_id   UInt8,
	            action_time  DateTime,
				f32 Float32,
				f64 Float64,
				str String
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
	*/
	rows, err := connect.Query("SELECT country_code, os_id, browser_id, action_time, f32, f64, str FROM example WHERE browser_id = ? or browser_id = ?", 88, 89)
	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		var (
			country     string
			os, browser uint8
			actionTime  time.Time
			f32         float32
			f64         float64
			str         string
		)
		if err := rows.Scan(&country, &os, &browser, &actionTime, &f32, &f64, &str); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, action_time: %s, f32: %f, f64: %f,str %s", country, os, browser, actionTime, f32, f64, str)
	}

	//return
	stmt, err := connect.Prepare("insert into example (os_id, browser_id) values (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	stmt.Exec(uint8(44), uint8(88))

	time.Sleep(time.Second * 10)

	/*
		if _, err := connect.Exec("DROP TABLE example"); err != nil {
			log.Fatal(err)
		}*/
}
