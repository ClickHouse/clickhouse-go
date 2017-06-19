package main

import (
	"database/sql/driver"
	"log"
	"time"

	"github.com/kshvakov/clickhouse"
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
				os_id        UInt8,
				action_day   Date,
				tags         Array(String),
				categories   Array(UInt8)
			) engine=Memory
		`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		tx.Commit()
	}
	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare("INSERT INTO example (os_id, action_day, tags, categories) VALUES (?, ?, ?, ?)")
		cstmt, ok := stmt.(clickhouse.ColumnarStatement)
		if !ok {
			log.Fatal("Column writer is not supported")
		}

		w := cstmt.ColumnWriter()
		for i := 0; i < 100; i++ {
			w.WriteUInt8(0, uint8(10+i))
		}

		for i := 0; i < 100; i++ {
			w.WriteDate(1, time.Now())
		}

		for i := 0; i < 100; i++ {
			w.WriteArray(2, clickhouse.Array([]string{"A", "B", "C"}))
		}

		for i := 0; i < 100; i++ {
			w.WriteArray(3, clickhouse.Array([]uint8{1, 2, 3, 4, 5}))
		}

		if err := cstmt.ColumnWriterEnd(100); err != nil {
			log.Fatal(err)
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
