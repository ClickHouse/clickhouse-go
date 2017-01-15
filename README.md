# ClickHouse

Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/)

## Key features

* Uses native ClickHouse tcp client-server protocol
* Compatibility with `database/sql`
* Support compression 
* Round Robin load-balancing

## DSN 

* timeout - timeout in seccond
* compress - disable/enable compression 
* username/password - auth credentials
* alt_hosts - comma separated list of single address host for load-balancing

example:
```
tcp://host1:9000?timeout=60&compress=true&username=user&password=qwerty&alt_hosts=host2:9000,host3:9000
```


## Install
```
go get -u github.com/kshvakov/clickhouse
```

## Example
```go 
package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/kshvakov/clickhouse"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000")
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
		tx, _   = connect.Begin() // it's not real transaction
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, action_time) VALUES (?, ?, ?, ?)")
	)
	// write to buffer
	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec("RU", 10+i, 100+i, time.Now()); err != nil {
			log.Fatal(err)
		}
	}
	// send batch request
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
```

Use [sqlx](https://github.com/jmoiron/sqlx)

```go
package main

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
)

func main() {
	connect, err := sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?compress=true&debug=true")
	if err != nil {
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
}
```