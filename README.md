# ClickHouse
[![run-tests](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml/badge.svg?branch=v2)](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/ClickHouse/clickhouse-go)](https://goreportcard.com/report/github.com/ClickHouse/clickhouse-go)

Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/). Supported by [Kinescope](https://kinescope.io).

## Key features

* Uses native ClickHouse TCP client-server protocol
* Compatibility with `database/sql` (slower than native interface!)
* Load-balancing
* Bulk write support (for `database/sql` use `begin->prepare->(in loop exec)->commit`)
* LZ4 compression support
* External Tables support

# `database/sql` interface

## DSN

* hosts  - comma-separated list of single address hosts for load-balancing
* username/password - auth credentials
* database - select the current default database
* dial_timeout -  A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix such as "300ms", "1s". Valid time units are "ms", "s", "m".
* connection_open_strategy - random/in_order (default random).
    * round-robin      - choose a round-robin server from the set
    * in_order    - first live server is chosen in specified order
* debug - enable debug output (boolean value)
* compress - enable lz4 compression (boolean value)
* check_connection_liveness - on supported platforms non-secure connections retrieved from the connection pool are checked in beginTx() for liveness before using them. If the check fails, the respective connection is marked as bad and the query retried with another connection. (boolean value, default is 'true')

SSL/TLS parameters:

* secure - establish secure connection (default is false)
* skip_verify - skip certificate verification (default is false)

Example:

```sh
clickhouse://username:password@host1:9000,host2:9000?&database=kinescope&dial_timeout=200ms
```

## TODO

* Support other compression methods(zstd ...)

## Install

```sh
go get -u github.com/ClickHouse/clickhouse-go
```

## Examples

```go
package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func example() error {
	conn,err:=
}

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
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

	_, err = connect.Exec(`
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
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time FROM example")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
```

### Use [sqlx](https://github.com/jmoiron/sqlx)

```go
package main

import (
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sqlx.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	var items []struct {
		CountryCode string    `db:"country_code"`
		OsID        uint8     `db:"os_id"`
		BrowserID   uint8     `db:"browser_id"`
		Categories  []int16   `db:"categories"`
		ActionTime  time.Time `db:"action_time"`
	}

	if err := connect.Select(&items, "SELECT country_code, os_id, browser_id, categories, action_time FROM example"); err != nil {
		log.Fatal(err)
	}

	for _, item := range items {
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_time: %s", item.CountryCode, item.OsID, item.BrowserID, item.Categories, item.ActionTime)
	}
}
```

### External tables support

```go
package main

import (
	"database/sql"
    "database/sql/driver"
	"fmt"
    "github.com/ClickHouse/clickhouse-go/lib/column"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
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

	_, err = connect.Exec(`
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
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare("INSERT INTO example (country_code, os_id, browser_id, categories, action_day, action_time) VALUES (?, ?, ?, ?, ?, ?)")
	)
	defer stmt.Close()

	for i := 0; i < 100; i++ {
		if _, err := stmt.Exec(
			"RU",
			10+i,
			100+i,
			clickhouse.Array([]int16{1, 2, 3}),
			time.Now(),
			time.Now(),
		); err != nil {
			log.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	col, err := column.Factory("country_code", "String", nil)
	if err != nil {
		log.Fatal(err)
	}
	countriesExternalTable := clickhouse.ExternalTable{
		Name: "countries",
		Values: [][]driver.Value{
			{"RU"},
		},
		Columns: []column.Column{col},
	}

    rows, err := connect.Query("SELECT country_code, os_id, browser_id, categories, action_day, action_time "+
            "FROM example WHERE country_code IN ?", countriesExternalTable)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			country               string
			os, browser           uint8
			categories            []int16
			actionDay, actionTime time.Time
		)
		if err := rows.Scan(&country, &os, &browser, &categories, &actionDay, &actionTime); err != nil {
			log.Fatal(err)
		}
		log.Printf("country: %s, os: %d, browser: %d, categories: %v, action_day: %s, action_time: %s", country, os, browser, categories, actionDay, actionTime)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	if _, err := connect.Exec("DROP TABLE example"); err != nil {
		log.Fatal(err)
	}
}
```
