# ClickHouse [![run-tests](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml/badge.svg?branch=v2)](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/ClickHouse/clickhouse-go/v2.svg)](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)

Golang SQL database driver for [Yandex ClickHouse](https://clickhouse.yandex/). Supported by [Kinescope](https://kinescope.io).

## Key features

* Uses native ClickHouse TCP client-server protocol
* Compatibility with [`database/sql`](#std-databasesql-interface) ([slower](#benchmark) than [native interface](#native-interface)!)
* Marshal rows into structs ([ScanStruct](tests/scan_struct_test.go), [Select](examples/native/scan_struct/main.go))
* Unmarshal struct to row ([AppendStruct](benchmark/v2/write-native-struct/main.go))
* Connection pool
* Failover and load balancing
* [Bulk write support](examples/native/batch/main.go) (for `database/sql` [use](examples/std/batch/main.go) `begin->prepare->(in loop exec)->commit`)
* [AsyncInsert](benchmark/v2/write-async/main.go)
* Named and numeric placeholders support
* LZ4 compression support
* External data

Support for the ClickHouse protocol advanced features using `Context`:

* Query ID
* Quota Key
* Settings
* OpenTelemetry
* Execution events:
	* Logs
	* Progress
	* Profile info
	* Profile events

# `database/sql` interface

## OpenDB

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
	Addr: []string{"127.0.0.1:9999"},
	Auth: clickhouse.Auth{
		Database: "default",
		Username: "default",
		Password: "",
	},
	TLS: &tls.Config{
		InsecureSkipVerify: true,
	},
	Settings: clickhouse.Settings{
		"max_execution_time": 60,
	},
	DialTimeout: 5 * time.Second,
	Compression: &clickhouse.Compression{
		clickhouse.CompressionLZ4,
	},
	Debug: true,
})
conn.SetMaxIdleConns(5)
conn.SetMaxOpenConns(10)
conn.SetConnMaxLifetime(time.Hour)
```
## DSN

* hosts  - comma-separated list of single address hosts for load-balancing and failover
* username/password - auth credentials
* database - select the current default database
* dial_timeout -  a duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix such as "300ms", "1s". Valid time units are "ms", "s", "m".
* connection_open_strategy - random/in_order (default random).
    * round-robin      - choose a round-robin server from the set
    * in_order    - first live server is chosen in specified order
* debug - enable debug output (boolean value)
* compress - enable lz4 compression (boolean value)

SSL/TLS parameters:

* secure - establish secure connection (default is false)
* skip_verify - skip certificate verification (default is false)

Example:

```sh
clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60
```

## Benchmark

| [V1 (READ)](benchmark/v1/read/main.go) | [V2 (READ) std](benchmark/v2/read/main.go) | [V2 (READ) native](benchmark/v2/read-native/main.go) |
| -------------------------------------- | ------------------------------------------ | ---------------------------------------------------- |
| 1.218s                                 | 924.390ms                                  | 675.721ms                                            |


| [V1 (WRITE)](benchmark/v1/write/main.go) | [V2 (WRITE) std](benchmark/v2/write/main.go) | [V2 (WRITE) native](benchmark/v2/write-native/main.go) | [V2 (WRITE) by column](benchmark/v2/write-native-columnar/main.go) |
| ---------------------------------------- | -------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------ |
| 1.899s                                   | 1.177s                                       | 699.203ms                                              | 661.973ms                                                          |



## Install

```sh
go get -u github.com/ClickHouse/clickhouse-go/v2
```

## Examples

### native interface

* [batch](examples/native/batch/main.go)
* [async insert](examples/native/write-async)
* [batch struct](examples/native/write-struct/main.go)
* [columnar](examples/native/write-columnar/main.go)
* [scan struct](examples/native/scan_struct/main.go)
* [bind params](examples/native/bind/main.go)

### std `database/sql` interface

* [batch](examples/std/batch/main.go)
* [async insert](examples/std/write-async)
* [open db](examples/std/open_db/main.go)
* [bind params](examples/std/bind/main.go)


## Alternatives

* Database drivers
	* [mailru/go-clickhouse](https://github.com/mailru/go-clickhouse) (uses the HTTP protocol)
	* [uptrace/go-clickhouse](https://github.com/uptrace/go-clickhouse) (uses the native TCP protocol with `database/sql`-like API)
	* drivers with columnar interface :
		* [vahid-sohrabloo/chconn](https://github.com/vahid-sohrabloo/chconn)
		* [go-faster/ch](https://github.com/go-faster/ch)

* Insert collectors:
	* [KittenHouse](https://github.com/YuriyNasretdinov/kittenhouse)
	* [nikepan/clickhouse-bulk](https://github.com/nikepan/clickhouse-bulk)

### Useful projects

* [clickhouse-backup](https://github.com/AlexAkulov/clickhouse-backup)
* [go-graphite](https://github.com/go-graphite)