package main

import (
	"database/sql/driver"
	"math/rand"

	"time"

	"log"

	"github.com/kshvakov/clickhouse"
)

func mustGetConn() driver.Conn {
	r, err := clickhouse.Open("tcp://127.0.0.1:9000/")
	if err != nil {
		panic(err)
	}

	return r
}

func mustExec(str string) {
	c := mustGetConn()
	defer c.Close()

	tx, _ := c.Begin()
	r, err := c.Prepare(str)
	if err != nil {
		panic(err)
	}

	_, err = r.Exec([]driver.Value{})
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

}

func initSchema() {
	mustExec("DROP TABLE IF EXISTS t1")
	mustExec(`CREATE TABLE IF NOT EXISTS t1 (  
		cid UInt16,		
		ts DateTime,
		date MATERIALIZED toDate(ts)
		) ENGINE = MergeTree(date, (cid), 8192)`)
}

var requests = []string{
	"INSERT INTO t1 (cid, ts) values (?, ?)",
}

func getTemplate(conn driver.Conn, reqNum int) driver.Stmt {
	r, err := conn.Prepare(requests[reqNum])
	if err != nil {
		panic(err)
	}

	return r
}

func worker(reqNum int) {
	c := mustGetConn()
	d, _ := time.Parse("2006-01-02", "2016-03-03")

	for {
		perIteration := rand.Intn(10)*10 + rand.Intn(8)*100 + 1
		tx, err := c.Begin()

		if err != nil {
			panic(err)
		}
		if rand.Intn(150) > 91 {
			d = d.Add(1 * time.Hour * 24)
		}
		q := getTemplate(c, reqNum)

		var cid = uint16(rand.Intn(200))

		for i := 0; i < perIteration; i++ {
			q.Exec([]driver.Value{
				cid,
				d,
			})
		}

		err = tx.Commit()

		if err != nil {
			panic(err)
		}

	}

}

const insertsPerIteration = 10000

const testRuntime = 5 * time.Minute

func main() {
	initSchema()

	for i := 0; i < len(requests); i++ {
		go worker(i)
	}

	log.Printf("Test is running...")
	time.Sleep(testRuntime)
}
