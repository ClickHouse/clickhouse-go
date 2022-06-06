# ClickHouse [![run-tests](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml/badge.svg?branch=v2)](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/ClickHouse/clickhouse-go/v2.svg)](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)

Golang SQL database driver for [ClickHouse](https://clickhouse.com/).

## Versions

There are two version of this driver, v1 and v2, available as separate branches. 

**v1 is now in a state of a maintenance - we will only accept PRs for bug and security fixes.**

Users should use v2 which is production ready and [significantly faster than v1](#benchmark).

## Supported ClickHouse Versions

The driver is tested against the currently [supported versions](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md) of ClickHouse

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


#### A Note on TLS/SSL

At a low level all driver connect methods (DSN/OpenDB/Open) will use the [Go tls package](https://pkg.go.dev/crypto/tls) to establish a secure connection. The driver knows to use TLS if the Options struct contains a non-nil tls.Config pointer.

Setting secure in the DSN creates a minimal tls.Config struct with only the InsecureSkipVerify field set (either true or false).  It is equivalent to this code:

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
	...
    TLS: &tls.Config{
            InsecureSkipVerify: false
	}
	...
    })
```
This minimal tls.Config is normally all that is necessary to connect to the secure native port (normally 9440) on a ClickHouse server. If the ClickHouse server does not have a valid certificate (expired, wrong host name, not signed by a publicly recognized root Certificate Authority), InsecureSkipVerify can be to `true`, but that is strongly discouraged.

If additional TLS parameters are necessary the application code should set the desired fields in the tls.Config struct. That can include specific cipher suites, forcing a particular TLS version (like 1.2 or 1.3), adding an internal CA certificate chain, adding a client certificate (and private key) if required by the ClickHouse server, and most of the other options that come with a more specialized security setup.

## Third-party alternatives

* Database drivers:
	* [mailru/go-clickhouse](https://github.com/mailru/go-clickhouse) (uses the HTTP protocol)
	* [uptrace/go-clickhouse](https://github.com/uptrace/go-clickhouse) (uses the native TCP protocol with `database/sql`-like API)
	* Drivers with columnar interface:
		* [vahid-sohrabloo/chconn](https://github.com/vahid-sohrabloo/chconn)
		* [go-faster/ch](https://github.com/go-faster/ch)

* Insert collectors:
	* [KittenHouse](https://github.com/YuriyNasretdinov/kittenhouse)
	* [nikepan/clickhouse-bulk](https://github.com/nikepan/clickhouse-bulk)
