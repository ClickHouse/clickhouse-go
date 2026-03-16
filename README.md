# ClickHouse [![run-tests](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml/badge.svg?branch=v2)](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/ClickHouse/clickhouse-go/v2.svg)](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)

Golang SQL database client for [ClickHouse](https://clickhouse.com/).

## Key features

* Uses ClickHouse native format for optimal performance. Utilises low level [ch-go](https://github.com/ClickHouse/ch-go) client for encoding/decoding and compression (versions >= 2.3.0).
* Supports native ClickHouse TCP client-server protocol
* Compatibility with [`database/sql`](#std-databasesql-interface) ([slower](#benchmark) than [native interface](#native-interface)!)
* [`database/sql`](#std-databasesql-interface) supports http protocol for transport.
* Marshal rows into structs ([ScanStruct](examples/clickhouse_api/scan_struct.go), [Select](examples/clickhouse_api/select_struct.go))
* Unmarshal struct to row ([AppendStruct](benchmark/v2/write-native-struct/main.go))
* Connection pool (for both TCP-Native and HTTP)
* Failover and load balancing
* [Bulk write support](examples/clickhouse_api/batch.go) (for `database/sql` [use](examples/std/batch.go) `begin->prepare->(in loop exec)->commit`)
* [PrepareBatch options](#preparebatch-options)
* [AsyncInsert](benchmark/v2/write-async/main.go) (more details in [Async insert](#async-insert) section)
* Named and numeric placeholders support
* LZ4/ZSTD compression support
* External data
* [Query parameters](examples/std/query_parameters.go)

Support for the ClickHouse protocol advanced features using `Context`:

* Query ID
* Quota Key
* Settings
* [Query parameters](examples/clickhouse_api/query_parameters.go)
* OpenTelemetry
* Execution events:
	* Logs
	* Progress
	* Profile info
	* Profile events


## Supported ClickHouse Versions

The client is tested against the currently [supported versions](https://github.com/ClickHouse/ClickHouse/blob/master/SECURITY.md) of ClickHouse

## Supported Golang Versions

| Client Version | Golang Versions        |
|----------------|------------------------|
| => 2.0 <= 2.2  | 1.17, 1.18             |
| >= 2.3         | 1.18.4+, 1.19          |
| >= 2.14        | 1.20, 1.21             |
| >= 2.19        | 1.21, 1.22             |
| >= 2.28        | 1.22, 1.23             |
| >= 2.29        | 1.21, 1.22, 1.23, 1.24 |

## Documentation

[https://clickhouse.com/docs/en/integrations/go](https://clickhouse.com/docs/en/integrations/go)

# `clickhouse` interface (formally `native` interface)

```go
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialContext: func(ctx context.Context, addr string) (net.Conn, error) {
			dialCount++
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		},
		Debug: true,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		BlockBufferSize: 10,
		MaxCompressionBuffer: 10240,
		ClientInfo: clickhouse.ClientInfo{ // optional, please see Client info section in the README.md
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "my-app", Version: "0.1"},
			},
		},
	})
	if err != nil {
		return err
	}
	return conn.Ping(context.Background())
```

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
	DialTimeout: time.Second * 30,
	Compression: &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	},
	Debug: true,
	BlockBufferSize: 10,
	MaxCompressionBuffer: 10240,
	ClientInfo: clickhouse.ClientInfo{ // optional, please see Client info section in the README.md
		Products: []struct {
			Name    string
			Version string
		}{
			{Name: "my-app", Version: "0.1"},
		},
	},
})
conn.SetMaxIdleConns(5)
conn.SetMaxOpenConns(10)
conn.SetConnMaxLifetime(time.Hour)
```

## DSN

* hosts  - comma-separated list of single address hosts for load-balancing and failover
* username/password - auth credentials
* database - select the current default database
* dial_timeout -  a duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix such as "300ms", "1s". Valid time units are "ms", "s", "m". (default 30s)
* connection_open_strategy - random/round_robin/in_order (default in_order).
    * random      - choose random server from the set
    * round_robin - choose a round-robin server from the set
    * in_order    - first live server is chosen in specified order
* debug - enable debug output (boolean value)
* compress - specify the compression algorithm: `none` (default), `zstd`, `lz4`, `lz4hc`, `gzip`, `deflate`, `br`. If set to `true`, `lz4` will be used.
* compress_level - Level of compression (algorithm-specific, default is 3 when compression is enabled):
  - `gzip`/`deflate`: `-2` (Best Speed) to `9` (Best Compression)
  - `br`: `0` (Best Speed) to `11` (Best Compression)
  - `zstd`/`lz4`/`lz4hc`: ignored
* block_buffer_size - size of block buffer (default 2)
* read_timeout - a duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix such as "300ms", "1s". Valid time units are "ms", "s", "m" (default 5m).
* max_compression_buffer - max size (bytes) of compression buffer during column by column compression (default 10MiB)
* client_info_product - optional list (comma separated) of product name and version pair separated with `/`. This value will be pass a part of client info. e.g. `client_info_product=my_app/1.0,my_module/0.1` More details in [Client info](#client-info) section.
* http_proxy - HTTP proxy address

## Connection Settings Reference

The following connection settings are available in both DSN strings and the `clickhouse.Options` struct:

### Timeout Settings
* **dial_timeout** - Connection timeout for establishing a connection to the server (default: 30s)
* **read_timeout** - Timeout for reading server responses (default: 5m)

### Connection Pool Settings
* **max_open_conns** - Maximum number of open connections to the database (default: MaxIdleConns + 5)
* **max_idle_conns** - Maximum number of idle connections in the pool (default: 5)
* **conn_max_lifetime** - Maximum amount of time a connection may be reused (default: 1h)

### Connection Strategy
* **connection_open_strategy** - Strategy for selecting servers from the connection pool:
  * `in_order` - Choose the first available server in the specified order (default)
  * `round_robin` - Choose servers in a round-robin fashion
  * `random` - Choose a random server from the pool

### Compression Settings
* **compress** - Enable compression with a specific algorithm: `none`, `zstd`, `lz4`, `lz4hc`, `gzip`, `deflate`, `br`. If set to `true`, `lz4` will be used (default: `none`)
* **compress_level** - Compression level (algorithm-specific):
  * `gzip`/`deflate`: `-2` (Best Speed) to `9` (Best Compression)
  * `br`: `0` (Best Speed) to `11` (Best Compression)
  * `zstd`/`lz4`: ignored
* **max_compression_buffer** - Maximum size of compression buffer in bytes (default: 10MiB)

### Buffer Settings
* **block_buffer_size** - Size of block buffer (default: 2)

### Debug Settings
* **debug** - Enable debug output (boolean value)

### SSL/TLS Settings
* **secure** - Establish secure connection (default: false)
* **skip_verify** - Skip certificate verification (default: false)

### Client Information
* **client_info_product** - Comma-separated list of product name and version pairs (e.g., `my_app/1.0,my_module/0.1`)

### HTTP Settings
* **http_proxy** - HTTP proxy address for HTTP protocol connections

Example:

```sh
clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&read_timeout=30s&max_execution_time=60
```

### HTTP Support

The native format can be used over the HTTP protocol. This is useful in scenarios where users need to proxy traffic e.g. using [ChProxy](https://www.chproxy.org/) or via load balancers.

This can be achieved by modifying the DSN to specify the HTTP protocol.

```sh
http://host1:8123,host2:8123/database?dial_timeout=200ms&max_execution_time=60
```

Alternatively, use `OpenDB` and specify the interface type.

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
	Addr: []string{"127.0.0.1:8123"},
	Auth: clickhouse.Auth{
		Database: "default",
		Username: "default",
		Password: "",
	},
	Settings: clickhouse.Settings{
		"max_execution_time": 60,
	},
	DialTimeout: 30 * time.Second,
	Compression: &clickhouse.Compression{
		Method: clickhouse.CompressionLZ4,
	},
	Protocol:  clickhouse.HTTP,
})
```

#### Proxy support

HTTP proxy can be set in the DSN string by specifying the `http_proxy` parameter.
(make sure to URL encode the proxy address)

```sh
http://host1:8123,host2:8123/database?dial_timeout=200ms&max_execution_time=60&http_proxy=http%3A%2F%2Fproxy%3A8080
```

If you are using `clickhouse.OpenDB`, set the `HTTProxy` field in the `clickhouse.Options`.

An alternative way is to enable proxy by setting the `HTTP_PROXY` (for HTTP) or `HTTPS_PROXY` (for HTTPS) environment variables.
See more details in the [Go documentation](https://pkg.go.dev/net/http#ProxyFromEnvironment).

## Compression

ZSTD/LZ4 compression is supported over native and http protocols. This is performed column by column at a block level and is only used for inserts. Compression buffer size is set as `MaxCompressionBuffer` option.

If using `Open` via the std interface and specifying a DSN, compression can be enabled via the `compress` flag. Currently, this is a boolean flag which enables `LZ4` compression.

Other compression methods will be added in future PRs.

## TLS/SSL

At a low level all client connect methods (DSN/OpenDB/Open) will use the [Go tls package](https://pkg.go.dev/crypto/tls) to establish a secure connection. The client knows to use TLS if the Options struct contains a non-nil tls.Config pointer.

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

### HTTPS

To connect using HTTPS either:

- Use `https` in your dsn string e.g.

    ```sh
    https://host1:8443,host2:8443/database?dial_timeout=200ms&max_execution_time=60
    ```

- Specify the interface type as `HttpsInterface` e.g.

```go
conn := clickhouse.OpenDB(&clickhouse.Options{
	Addr: []string{"127.0.0.1:8443"},
	Auth: clickhouse.Auth{
		Database: "default",
		Username: "default",
		Password: "",
	},
	Protocol:  clickhouse.HTTP,
})
```

## Client info


Clickhouse-go implements [client info](https://docs.google.com/document/d/1924Dvy79KXIhfqKpi1EBVY3133pIdoMwgCQtZ-uhEKs/edit#heading=h.ah33hoz5xei2) as a part of language client specification. `client_name` for native protocol and HTTP `User-Agent` header values are provided with the exact client info string.

Users can extend client options with additional product information included in client info. This might be useful for analysis [on a server side](https://clickhouse.com/docs/en/operations/system-tables/query_log/).

Order is the highest abstraction to the lowest level implementation left to right.

Usage examples for [native API](examples/clickhouse_api/client_info.go) and [database/sql](examples/std/client_info.go)  are provided.

## Async insert

[Async insert](https://clickhouse.com/docs/optimize/asynchronous-inserts) is supported via `WithAsync()` helper on both Native and HTTP protocols. You can use it for both Go standard interface `OpenDB` and also ClickHouse interface `Open()`.

**NOTE**: You can use `WithSettings()` manually to add any async related settings. `WithAsync()` is just a simple wrapper that does that for you.

We have following examples to show Async Insert in action.
1. [Native with OpenDB](examples/clickhouse_api/async_native.go)
1. [HTTP with OpenDB](examples/clickhouse_api/async_http.go)
1. [Native with Open](examples/std/async_native.go)
1. [HTTP with Open](examples/std/async_http.go)

**NOTE**: The old `AsyncInsert()` api is deprecated and will be removed in future versions. We highly recommend to use `WithAsync()` api for all the Async Insert use cases.

## PrepareBatch options

Available options:
- [WithReleaseConnection](examples/clickhouse_api/batch_release_connection.go) - after PrepareBatch connection will be returned to the pool. It can help you make a long-lived batch.

## Benchmark

| [V2 (READ) std](benchmark/v2/read/main.go) | [V2 (READ) clickhouse API](benchmark/v2/read-native/main.go) |
| ------------------------------------------ |--------------------------------------------------------------|
| 924.390ms                                  | 675.721ms                                                    |


| [V2 (WRITE) std](benchmark/v2/write/main.go) | [V2 (WRITE) clickhouse API](benchmark/v2/write-native/main.go) | [V2 (WRITE) by column](benchmark/v2/write-native-columnar/main.go) |
| -------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------ |
| 1.177s                                       | 699.203ms                                              | 661.973ms                                                          |



## Install

```sh
go get -u github.com/ClickHouse/clickhouse-go/v2
```

## Examples

### native interface

* [batch](examples/clickhouse_api/batch.go)
* [batch with release connection](examples/clickhouse_api/batch_release_connection.go)
* [native async insert](examples/clickhouse_api/async_native.go)
* [http async insert](examples/clickhouse_api/async_http.go)
* [batch struct](examples/clickhouse_api/append_struct.go)
* [columnar](examples/clickhouse_api/columnar_insert.go)
* [scan struct](examples/clickhouse_api/scan_struct.go)
* [query parameters](examples/clickhouse_api/query_parameters.go)
* [bind params](examples/clickhouse_api/bind.go) (deprecated in favour of native query parameters)
* [client info](examples/clickhouse_api/client_info.go)

### std `database/sql` interface

* [batch](examples/std/batch.go)
* [native async insert](examples/std/async_native.go)
* [http async insert](examples/std/async_http.go)
* [open db](examples/std/connect.go)
* [query parameters](examples/std/query_parameters.go)
* [bind params](examples/std/bind.go) (deprecated in favour of native query parameters)
* [client info](examples/std/client_info.go)

## Third-party libraries

* [clickhouse-go-rows-utils](https://github.com/EpicStep/clickhouse-go-rows-utils) - utilities that simplify working with rows.

## ClickHouse alternatives - ch-go

Versions of this client >=2.3.x utilise [ch-go](https://github.com/ClickHouse/ch-go) for their low level encoding/decoding. This low lever client provides a high performance columnar interface and should be used in performance critical use cases. This client provides more familar row orientated and `database/sql` semantics at the cost of some performance.

Both clients are supported by ClickHouse.

## Third-party alternatives

* Database client/clients:
	* [mailru/go-clickhouse](https://github.com/mailru/go-clickhouse) (uses the HTTP protocol)
	* [uptrace/go-clickhouse](https://github.com/uptrace/go-clickhouse) (uses the native TCP protocol with `database/sql`-like API)
	* Drivers with columnar interface:
		* [vahid-sohrabloo/chconn](https://github.com/vahid-sohrabloo/chconn)

* Insert collectors:
	* [KittenHouse](https://github.com/YuriyNasretdinov/kittenhouse)
	* [nikepan/clickhouse-bulk](https://github.com/nikepan/clickhouse-bulk)
