# clickhouse

Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/)

## Notice 

Current version of driver uses [ClickHouse HTTP interface](https://clickhouse.yandex/reference_en.html#Interfaces)

## Key features

* Compatibility with `database/sql`
* Support for gzip compression 
* Round Robin load-balancing and fallback 

## DSN 

* timeout - timeout in seccond
* compress - disable/enable gzip compression 
* username/password - auth credentials
* alt_hosts - comma separated list of single address host for load-balancing and fallback 

example: http://127.0.0.1:8123?timeout=60&compress=true&username=user&password=qwerty&alt_hosts=host2:8123,host3:8123


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
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123")
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
		if _, err := stmt.Exec("RU", 100+i, 200+i, time.Now()); err != nil {
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
	connect, err := sqlx.Open("clickhouse", "http://127.0.0.1:8123?compress=true&debug=true")
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