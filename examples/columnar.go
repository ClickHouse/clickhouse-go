package main

import (
	"database/sql/driver"
	"log"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	data "github.com/ClickHouse/clickhouse-go/lib/data"
)

func main() {
	connect, err := clickhouse.OpenDirect("tcp://127.0.0.1:9000?username=&debug=true&compress=1")
	if err != nil {
		log.Fatal(err)
	}
	{
		connect.Begin()
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

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		connect.Begin()
		connect.Prepare("INSERT INTO example (os_id, action_day, tags, categories) VALUES (?, ?, ?, ?)")

		block, err := connect.Block()
		if err != nil {
			log.Fatal(err)
		}

		blocks := []*data.Block{block, block.Copy()}

		var wg sync.WaitGroup
		wg.Add(len(blocks))

		for i := range blocks {
			b := blocks[i]
			go func() {
				defer wg.Done()
				writeBatch(b, 1000)
				if err := connect.WriteBlock(b); err != nil {
					log.Fatal(err)
				}
			}()
		}

		wg.Wait()

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		connect.Begin()
		stmt, _ := connect.Prepare(`SELECT count() FROM example`)

		rows, err := stmt.Query([]driver.Value{})
		if err != nil {
			log.Fatal(err)
		}

		columns := rows.Columns()
		row := make([]driver.Value, 1)
		for rows.Next(row) == nil {
			for i, c := range columns {
				log.Print(c, " : ", row[i])
			}
		}

		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
	{
		connect.Begin()
		stmt, _ := connect.Prepare(`DROP TABLE example`)
		if _, err := stmt.Exec([]driver.Value{}); err != nil {
			log.Fatal(err)
		}
		if err := connect.Commit(); err != nil {
			log.Fatal(err)
		}
	}
}

func writeBatch(block *data.Block, n int) {
	block.Reserve()
	block.NumRows += uint64(n)

	for i := 0; i < n; i++ {
		block.WriteUInt8(0, uint8(10+i))
	}

	for i := 0; i < n; i++ {
		block.WriteDate(1, time.Now())
	}

	for i := 0; i < n; i++ {
		block.WriteArray(2, clickhouse.Array([]string{"A", "B", "C"}))
	}

	for i := 0; i < n; i++ {
		block.WriteArray(3, clickhouse.Array([]uint8{1, 2, 3, 4, 5}))
	}
}
