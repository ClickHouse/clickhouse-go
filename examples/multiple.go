package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?username=&debug=true")
	checkErr(err)
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return
	}

	for i := 0; i < 10; i++ {
		connect.Exec(fmt.Sprintf("DROP TABLE IF EXISTS example_%d", i))
		_, err = connect.Exec(fmt.Sprintf(`
			CREATE TABLE example_%d (
				country_code FixedString(2),
				os_id        UInt8,
				browser_id   UInt8,
				categories   Array(Int16),
				action_day   Date,
				action_time  DateTime
			) engine=Memory
		`, i))

		checkErr(err)
	}
	for i := 0; i < 10; i++ {
		go func(i int) {
			for {
				tx, err := connect.Begin()
				checkErr(err)
				stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO example_%d (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)", i))
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
				time.Sleep(time.Second)
			}
		}(i)
	}

	<-time.Tick(time.Minute)
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
