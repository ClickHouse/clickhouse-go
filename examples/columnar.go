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
			) engine=Memory
		`)

		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		tx.Commit()
	}
	{
		tx, _ := connect.Begin()
		stmt, _ := connect.Prepare("INSERT INTO example (os_id, action_day) VALUES (?, ?)")
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
