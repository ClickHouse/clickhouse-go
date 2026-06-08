# ClickHouse [![run-tests](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml/badge.svg?branch=v2)](https://github.com/ClickHouse/clickhouse-go/actions/workflows/run-tests.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/ClickHouse/clickhouse-go/v2.svg)](https://pkg.go.dev/github.com/ClickHouse/clickhouse-go/v2)

Golang SQL database client for [ClickHouse](https://clickhouse.com/).

## Install

```sh
go get github.com/ClickHouse/clickhouse-go/v2
```

## Which interface should I use?

| | `clickhouse.Open` (native) | `sql.Open` / `clickhouse.OpenDB` (std) |
|---|---|---|
| Performance | Faster (direct column encoding) | Slower (see [benchmark](#benchmark)) |
| API | `driver.Conn` — ClickHouse-specific | Standard `database/sql` |
| Use when | new code, performance-sensitive work | existing `database/sql` tooling, ORMs |

Both support TCP and HTTP transport. When in doubt, use the native interface.

## Key features

* Uses ClickHouse native format for optimal performance. Utilises low level [ch-go](https://github.com/ClickHouse/ch-go) client for encoding/decoding and compression (versions >= 2.3.0).
* Supports both native ClickHouse TCP and HTTP client-server protocols
* Compatibility with [`database/sql`](#std-databasesql-interface) ([slower](#benchmark) than [native interface](#native-interface)!)
* [`database/sql`](#std-databasesql-interface) supports both native TCP and HTTP protocols for transport.
* Marshal rows into structs ([ScanStruct](examples/clickhouse_api/scan_struct.go), [Select](examples/clickhouse_api/select_struct.go))
* Unmarshal struct to row ([AppendStruct](benchmark/v2/write-native-struct/main.go))
* Connection pool (for both TCP-Native and HTTP)
* Failover and load balancing
* [Bulk write support](examples/clickhouse_api/batch.go) (for `database/sql` [use](examples/std/batch.go) `begin->prepare->(in loop exec)->commit`)
* [PrepareBatch options](#preparebatch-options)
* [AsyncInsert](benchmark/v2/write-async/main.go) (more details in [Async insert](#async-insert) section)
* Named and numeric placeholders support
* LZ4/ZSTD/LZ4HC/GZIP/Deflate/Brotli compression support
* External data
* [Query parameters](examples/std/query_parameters.go)
* Structured logging via `log/slog` ([Logger option](#logging))
* JWT authentication support
* Wide type support: BFloat16, QBit, Dynamic, Variant, Time, Time64, LineString, MultiLineString, and more

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
| >= 2.0 <= 2.2  | 1.17, 1.18             |
| >= 2.3         | 1.18.4+, 1.19          |
| >= 2.14        | 1.20, 1.21             |
| >= 2.19        | 1.21, 1.22             |
| >= 2.28        | 1.22, 1.23             |
| >= 2.29        | 1.21, 1.22, 1.23, 1.24 |
| >= 2.41        | 1.24, 1.25             |

## Documentation

[https://clickhouse.com/docs/en/integrations/go](https://clickhouse.com/docs/en/integrations/go)

# `clickhouse` interface (formerly `native` interface)

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
		// Logger is the recommended way to enable logging (see Logging section).
		// Debug and Debugf are deprecated in favour of Logger.
		Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
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
	// Debug is deprecated; use Logger instead (see Logging section).
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
* compress - specify the compression algorithm: `none` (default), `zstd`, `lz4`, `lz4hc`, `gzip`, `deflate`, `br`. If set to `true`, `lz4` will be used. For HTTP connections, `gzip`/`deflate`/`br` use HTTP web compression, while `lz4`/`zstd` use ClickHouse native block compression over HTTP (`lz4hc` is native-only).
* compress_level - Level of compression (algorithm-specific, default is 3 when compression is enabled):
  - `gzip`/`deflate`: `-2` (Best Speed) to `9` (Best Compression)
  - `br`: `0` (Best Speed) to `11` (Best Compression)
  - `zstd`/`lz4`/`lz4hc`: ignored
* block_buffer_size - size of block buffer (default 2)
* read_timeout - a duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix such as "300ms", "1s". Valid time units are "ms", "s", "m" (default 5m).
* max_compression_buffer - max size (bytes) of compression buffer during column by column compression (default 10MiB)
* client_info_product - optional list (comma separated) of product name and version pair separated with `/`. This value will be pass a part of client info. e.g. `client_info_product=my_app/1.0,my_module/0.1` More details in [Client info](#client-info) section.
* http_proxy - HTTP proxy address
* http_path - URL path for HTTP requests (e.g. for proxies or custom endpoints that require a specific path)
* tls_server_name - set TLS SNI/verification name (sets `tls.Config.ServerName` when `secure=true`)

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
* **compress** - Enable compression with a specific algorithm: `none`, `zstd`, `lz4`, `lz4hc`, `gzip`, `deflate`, `br`. If set to `true`, `lz4` will be used (default: `none`). For HTTP connections, `gzip`/`deflate`/`br` use HTTP web compression, while `lz4`/`zstd` use ClickHouse native block compression over HTTP (`lz4hc` is native-only).
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

If you are using `clickhouse.OpenDB`, set the `HTTPProxyURL` field in the `clickhouse.Options`.

An alternative way is to enable proxy by setting the `HTTP_PROXY` (for HTTP) or `HTTPS_PROXY` (for HTTPS) environment variables.
See more details in the [Go documentation](https://pkg.go.dev/net/http#ProxyFromEnvironment).

## Compression

Compression is supported over native and HTTP protocols.

Native protocol supports `lz4`, `lz4hc`, and `zstd`.

HTTP protocol supports `lz4` and `zstd` via ClickHouse native block compression over HTTP, and `gzip`, `deflate`, and `br` via HTTP web compression.

### HTTP: Web Compression vs Native Block Compression

When using the HTTP protocol there are two independent compression layers:

1. **HTTP web compression** (whole request/response body). This uses HTTP headers (`Accept-Encoding` and `Content-Encoding`). In ClickHouse, response compression is controlled by the `enable_http_compression` setting (pass it via `Options.Settings` or DSN query params). In clickhouse-go this mode is used when `Compression.Method` is `gzip`, `deflate`, or `br`.

2. **ClickHouse native block compression over HTTP** (Native format blocks). This uses ClickHouse HTTP query parameters: `compress=1` (server compresses response blocks) and `decompress=1` (server expects a compressed request body). In clickhouse-go this mode is used when `Compression.Method` is `lz4` or `zstd`.

Avoid enabling both at the same time unless you've measured it, as it can waste CPU by compressing already-compressed native blocks.

Note: you normally don't need to set `compress=1` or `decompress=1` yourself when using clickhouse-go; selecting an appropriate `Compression.Method` will configure the HTTP request correctly.

When using a DSN, compression can be enabled via the `compress` parameter. Set it to a specific algorithm name (`zstd`, `lz4`, `lz4hc`, `gzip`, `deflate`, `br`) or to `true` as shorthand for `lz4`. See the [DSN](#dsn) section for details.

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

### Server Certificate SAN (Go)

Go does not fall back to the certificate Common Name (CN) for hostname verification. If your ClickHouse server certificate does not contain a matching Subject Alternative Name (SAN), you may see:

```text
tls: failed to verify certificate: x509: certificate relies on legacy Common Name field, use SANs instead
```

Fix: regenerate the **server** certificate with SANs matching how you connect (DNS and/or IP). For example:

```bash
openssl req -newkey rsa:2048 -nodes \
  -subj "/CN=clickhouse" \
  -addext "subjectAltName = DNS:clickhouse.local,IP:127.0.0.1" \
  -keyout clickhouse.key -out clickhouse.csr

openssl x509 -req -in clickhouse.csr -out clickhouse.crt \
  -CA CAroot.crt -CAkey CAroot.key -days 3650 -copy_extensions copy
```

If you must connect to an IP address but your certificate SAN only contains a DNS name, set `tls_server_name` in the DSN (or `tls.Config.ServerName` in code) to the DNS name in the certificate.

### HTTPS

To connect using HTTPS either:

- Use `https` in your dsn string e.g.

    ```sh
    https://host1:8443,host2:8443/database?dial_timeout=200ms&max_execution_time=60
    ```

- Use `Protocol: clickhouse.HTTP` with a `TLS` config e.g.

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

## Cluster interserver-secret authentication

Native protocol only. Lets a client authenticate as a trusted ClickHouse cluster peer using the cluster's shared secret instead of a user password, then run each query as an arbitrary `initial_user` chosen per-call. This is the same wire protocol that ClickHouse itself uses for distributed queries.

> **Read [Security model](#security-model) before adopting this feature.** The cluster secret authorizes impersonation of any user on the cluster — including superusers — and so any process holding it must be treated as a cluster-admin-equivalent service.

### When to use it

- You operate the ClickHouse cluster yourself (you control `<remote_servers><secret>…</secret></remote_servers>`).
- You run a *trusted internal service* — query router, multi-tenant gateway, internal admin tool — that authenticates end-users at its own layer and needs to attribute queries back to those users in `system.query_log`.
- You target ClickHouse versions older than 25.11 (where `EXECUTE AS` does not exist) or want a single mechanism that works across the whole supported range.

This feature is **not** intended to be exposed to arbitrary end-user clients.

### Usage

Configure on `Options` (never via DSN — see *Why not DSN* below):

```go
conn, err := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"clickhouse:9440"},
    TLS:  &tls.Config{ServerName: "clickhouse.internal"}, // strongly recommended
    Auth: clickhouse.Auth{
        Database: "default",
        Username: "router_fallback_user", // fallback when WithInitialUser is not set; cannot default to "default"
    },
    Cluster: clickhouse.ClusterCredentials{
        Name:   "my_cluster",                                  // matches <remote_servers> entry
        Secret: os.Getenv("CLICKHOUSE_CLUSTER_SECRET"),
    },
})

ctx := clickhouse.Context(context.Background(),
    clickhouse.WithInitialUser("alice"),
)
rows, err := conn.Query(ctx, "SELECT 1")
```

Server side, the named cluster must have a `<secret>` matching what the client sends:

```xml
<remote_servers>
    <my_cluster>
        <secret>same-secret-as-client</secret>
        <shard><replica><host>clickhouse</host><port>9000</port></replica></shard>
    </my_cluster>
</remote_servers>
```

The user named in `WithInitialUser` (or `Auth.Username` if no `WithInitialUser` is set) must exist on the server. The server runs the query as that user without a password check, since the cluster-secret signature already proves the caller is trusted.

Each query is signed with `SHA256(salt + secret + body + id + initial_user)` (the layout `TCPHandler::processQuery` expects). The driver advertises protocol revision 54460, which omits the V2 nonce/external-roles fields added in 54462+ — sufficient for typical impersonation use, and compatible with any modern server.

A runnable example: [examples/clickhouse_api/cluster_secret.go](examples/clickhouse_api/cluster_secret.go).

### Security model

**The cluster secret is the highest-privilege credential in your ClickHouse deployment.** Anything that holds it can become any user on the cluster, including superusers. Treat any process holding it as a cluster-admin-equivalent service.

Concrete operating practices:

- **Source the secret from a secret manager**, not from environment variables baked into container images. Rotate the secret on any application compromise; rotation is cluster-wide (every server config and every client app must be updated together).
- **Scope blast radius with per-cluster-name secrets.** ClickHouse `<remote_servers>` accepts multiple cluster entries; give each trusted application its own `<my_app_cluster><secret>…</secret></my_app_cluster>` so a leaked secret only impersonates within that one application boundary, not across the whole cluster.
- **Prefer per-cluster-name secrets over per-tenant** if you operate multi-tenant infrastructure. The wire protocol does not give the server a way to scope which `initial_user` a holder of a given secret may claim — that is an *operational* boundary, not a *cryptographic* one.
- **Require TLS for any non-loopback connection.** The protocol the driver speaks is V1 (no nonce); without TLS, an on-path attacker who captures a signed query frame can replay it on the same connection. The driver emits a `Warn` log line when interserver-secret mode is used without TLS.
- **Audit `system.query_log` regularly** for unexpected `(user, address)` combinations. Interserver-secret queries appear with `is_initial_query = 0` and the impersonated user in both `user` and `initial_user`, so they are distinguishable from normal logins.
- **Never put the secret in a DSN.** See *Why not DSN* below.

The driver enforces fail-closed defaults at `Open()`:

| Misconfiguration | Sentinel error |
|---|---|
| `Cluster.Secret` set without `Cluster.Name` | `ErrClusterSecretRequiresName` |
| `Cluster.Secret` set with `Protocol: HTTP` | `ErrClusterSecretNeedsNative` |
| `Cluster.Secret` set without an explicit `Auth.Username` | `ErrClusterSecretRequiresUsername` |

The third check exists because `Auth.Username` defaults to `"default"` when blank. Without that check, a caller who configures `Cluster.Secret` and forgets `WithInitialUser` would silently run queries as `default` — typically a superuser. The driver therefore requires you to name the fallback user explicitly.

`ClusterCredentials.String()` and `GoString()` redact `Secret`, so accidental logging via `slog.Any("opt", opt)` or `fmt.Sprintf("%+v", opt)` cannot leak it.

### Why not DSN

`Cluster.Secret` is sensitive cluster-wide credential material. DSNs are passed as connection strings to `database/sql`, and frequently end up in startup logs, error messages, config files, and stack traces. The driver intentionally has no DSN parameter for the cluster secret — configure it via `Options{}` only, sourcing the value from a secret manager or env var that your process loads at startup.

### Comparison with `EXECUTE AS`

ClickHouse 25.11 introduced [`EXECUTE AS`](https://clickhouse.com/docs/sql-reference/statements/execute_as) for in-SQL impersonation. It is the right choice for many cases, but the two features have different tradeoffs:

| | Cluster interserver-secret (this feature) | `EXECUTE AS` |
|---|---|---|
| Server version | Stable since ClickHouse 21.6 (revision 54441) | 25.11+ |
| Server config | Requires `<remote_servers><my_cluster><secret>...</secret></my_cluster></remote_servers>` in the server config (often already present in clustered deployments). | Requires `GRANT IMPERSONATE` always; on 25.11–26.2 also requires `access_control_improvements.allow_impersonate_user = 1` ([relocated to that section in 26.2](https://github.com/ClickHouse/ClickHouse/pull/96451), [enabled by default in 26.3 LTS](https://github.com/ClickHouse/ClickHouse/pull/97870)). Both features need *some* server-side config — neither is config-free. |
| Authorization grain | **Coarse**: one secret per `<cluster>` entry impersonates *any* user that exists on the server. Scoping is operational only (per-cluster-name in `<remote_servers>`). | **Fine**: `GRANT IMPERSONATE ON <user> TO <holder>` is per-target-user; `GRANT IMPERSONATE ON * TO <holder>` is the broad form. Cryptographically scoped by SQL grants. |
| Holding identity | A 32+ byte shared secret in the application's process memory, sourced from a secret manager. | A SQL user that owns only `GRANT IMPERSONATE` (no other privileges required). Authenticates with any normal mechanism — password, certificate, JWT. |
| Credential rotation | Cluster-wide: every server config and every client app must update together. | Per-holder: rotate that user's password/JWT/cert independently of cluster config. |
| Connection model | Per-query identity carried in a protocol header field | Session-level (`EXECUTE AS u;`) or wraps each SQL statement (`EXECUTE AS u SELECT …`) |
| Connection pooling | Reuses connections across users freely | Session-level `EXECUTE AS` makes pool reuse hard; per-query form forces SQL surgery on every call |
| Parameterized queries | Works with `Conn.QueryRow(ctx, "SELECT ?")` | The literal SQL must start with `EXECUTE AS`, so binding has to be reworked |
| Identity in query text | No — identity is in a protocol field, no SQL injection surface for the impersonation identity. | Yes — `EXECUTE AS <user>` is part of the SQL text. Dynamic-SQL bugs in any layer become impersonation bugs. |
| Stability | Path that ClickHouse itself uses for every distributed query, exercised constantly | Recently shipped, with [open](https://github.com/ClickHouse/ClickHouse/issues/99572) [bugs](https://github.com/ClickHouse/ClickHouse/issues/100695) |
| Audit signal in `system.query_log` | `is_initial_query=0` plus the impersonated user in both `user` and `initial_user` — clear protocol-level marker. | `system.query_log.user` is the impersonated user; the actual authenticated user has to be recovered via the `authenticatedUser()` SQL function inside the query, or via `system.session_log` joined on `query_id`. |
| Audit signal of who held the credential | None inherent — interserver auth events do not produce a per-app `system.session_log` entry; you rely on application-side logs and `(initial_address, initial_user)` patterns. | `system.session_log` records the holder's logins, giving a per-credential-holder audit trail at the database layer. |

**Honest framing of the security trade.** `EXECUTE AS` has *better* authorization grain and a *better* per-holder audit story; cluster interserver-secret has a coarser scope by design. Neither credential is more or less phishable than the other in absolute terms — both live as material in process memory or a secret manager, both can be stolen, both should be rotated on compromise. The interserver-secret advantage is not "no credential to steal" — it is **operational**: connection-pool reuse, no SQL surgery, no SQL-injection surface for the identity field, version coverage that includes every supported ClickHouse release, and reuse of a code path that ClickHouse itself runs continuously.

In short: prefer `EXECUTE AS` when (a) you are on 25.11+, (b) impersonation pairs are few and stable enough to express as `GRANT IMPERSONATE` statements, (c) you want fine-grained per-pair authorization, and (d) you can tolerate the connection-pool friction. Reach for cluster interserver-secret when you need a uniform, version-portable mechanism in a trusted internal service that runs queries under many short-lived `initial_user` identities and is willing to accept the coarser grain in exchange for protocol-level efficiency.

## Client info


Clickhouse-go implements [client info](https://docs.google.com/document/d/1924Dvy79KXIhfqKpi1EBVY3133pIdoMwgCQtZ-uhEKs/edit#heading=h.ah33hoz5xei2) as a part of language client specification. `client_name` for native protocol and HTTP `User-Agent` header values are provided with the exact client info string.

Users can extend client options with additional product information included in client info. This might be useful for analysis [on a server side](https://clickhouse.com/docs/en/operations/system-tables/query_log/).

Order is the highest abstraction to the lowest level implementation left to right.

Usage examples for [native API](examples/clickhouse_api/client_info.go) and [database/sql](examples/std/client_info.go)  are provided.

## Logging

Structured logging is supported via Go's standard `log/slog` package. Set the `Logger` field in `Options` to enable it:

```go
conn, err := clickhouse.Open(&clickhouse.Options{
	Addr: []string{"127.0.0.1:9000"},
	Logger: slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})),
})
```

The `Debug` and `Debugf` fields in `Options` are deprecated in favour of `Logger`.

## Async insert

[Async insert](https://clickhouse.com/docs/optimize/asynchronous-inserts) is supported via `WithAsync()` helper on both Native and HTTP protocols. You can use it for both Go standard interface `OpenDB` and also ClickHouse interface `Open()`.

**NOTE**: You can use `WithSettings()` manually to add any async related settings. `WithAsync()` is just a simple wrapper that does that for you.

We have following examples to show Async Insert in action.
1. [Native with Open](examples/clickhouse_api/async_native.go)
1. [HTTP with Open](examples/clickhouse_api/async_http.go)
1. [Native with OpenDB](examples/std/async_native.go)
1. [HTTP with OpenDB](examples/std/async_http.go)

**NOTE**: The old `AsyncInsert()` api is deprecated and will be removed in future versions. We highly recommend to use `WithAsync()` api for all the Async Insert use cases.

## PrepareBatch options

Available options:
- [WithReleaseConnection](examples/clickhouse_api/batch_release_connection.go) - after PrepareBatch connection will be returned to the pool. It can help you make a long-lived batch.
- WithCloseOnFlush - close the current INSERT on each Flush and release the connection.

### Batch lifecycle (Flush vs Send vs Close)

For `clickhouse.Conn.PrepareBatch` (native interface):

- Use `Append`/`AppendStruct` to buffer rows client-side.
- Use `Flush` to send currently buffered rows while keeping the batch usable (native protocol). For HTTP protocol, `Flush` is currently a no-op.
- Use `Send` to flush any remaining rows and finalize the INSERT. After `Send`, the batch is considered sent and should not be reused.
- Use `defer batch.Close()` to ensure resources are released if `Send` is not reached.

## JSON columns: append contract

The ClickHouse Native protocol requires **one serialization version per `JSON` column per block** — a column cannot mix `object` rows and `string` rows on the wire. The driver enforces this at append time.

**Two modes, one per batch:**

- `object` — the driver decomposes a value into typed/dynamic paths. Accepts: `struct`, `map[string]any`, `*struct`, `*map`, `*clickhouse.JSON`, and any type implementing `clickhouse.JSONSerializer`.
- `string` — the driver stores raw JSON text. Accepts: `string`, `*string`, `[]byte`, `*[]byte`, `json.RawMessage`, `*json.RawMessage`, `sql.NullString`, `*sql.NullString`, and types implementing `driver.Valuer` or `fmt.Stringer`.

**Null rows are mode-agnostic.** `nil`, typed-nil pointers (`(*string)(nil)`, `(*clickhouse.JSON)(nil)`), `*interface{}` holding nil, and `sql.NullString{Valid: false}` do **not** latch a mode. They are buffered until a non-null row chooses the mode, and then flushed into the chosen backing column. `Nullable(JSON)` works the same way — the null mask lives on the Nullable wrapper; the inner JSON column still needs to emit something that parses server-side.

**The first non-null row picks the mode.** Subsequent rows must match:

```go
batch.Append(struct{ Name string }{"Alice"}) // latches "object"
batch.Append(`{"x":1}`)                      // error: string in an object-mode column
```

**Mixed-mode appends return an error**, identifying the type of the rejected row. There is no silent `{}` fallback.

**All-null batches** default to `string` mode at send time and encode each null row as the JSON literal `"null"` (smaller on the wire than an empty object, and valid JSON so the server accepts the payload in `Nullable(JSON)` String mode).

**Columnar bulk inserts (`batch.Column(i).Append(slice)`)** follow the same rules:
- `[]string`, `[]*string`, `[][]byte`, `[]*[]byte`, `[]json.RawMessage`, `[]*json.RawMessage`, `[]sql.NullString`, `[]*sql.NullString` → `string` mode.
- `[]struct{...}`, `[]map[string]any`, `[]clickhouse.JSON`, `[]*clickhouse.JSON`, `[]clickhouse.JSONSerializer` → `object` mode.
- `Append` expects a slice — passing a single scalar returns an error. Use `AppendRow` for per-row inserts.

## Benchmark

Indicative numbers measured on: Linux 6.19.6-arch1-1 · Intel Core Ultra 7 258V (8 cores) · 30 GiB RAM · NVMe SSD. Run the linked programs directly to get numbers on your hardware, e.g. `go run benchmark/v2/read/main.go`. Go benchmark tests can be run with `go test -bench=. ./benchmark/...`.

| [V2 (READ) std](benchmark/v2/read/main.go) | [V2 (READ) clickhouse API](benchmark/v2/read-native/main.go) |
| ------------------------------------------ |--------------------------------------------------------------|
| 883.196ms                                  | 731.359ms                                                    |


| [V2 (WRITE) std](benchmark/v2/write/main.go) | [V2 (WRITE) clickhouse API](benchmark/v2/write-native/main.go) | [V2 (WRITE) by column](benchmark/v2/write-native-columnar/main.go) |
| -------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------ |
| 604.953ms                                    | 368.245ms                                              | 581.322ms                                                          |



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
* [multi-host / failover](examples/clickhouse_api/multi_host.go)
* [bfloat16](examples/clickhouse_api/bfloat16.go)
* [dynamic](examples/clickhouse_api/dynamic.go)
* [variant](examples/clickhouse_api/variant.go)
* [qbit](examples/clickhouse_api/qbit.go)
* [json](examples/clickhouse_api/json_structs.go)
* [geo](examples/clickhouse_api/geo.go)
* [ephemeral columns (native)](examples/clickhouse_api/ephemeral_native.go)
* [ephemeral columns (http)](examples/clickhouse_api/ephemeral_http.go)

### std `database/sql` interface

* [batch](examples/std/batch.go)
* [native async insert](examples/std/async_native.go)
* [http async insert](examples/std/async_http.go)
* [open db](examples/std/connect.go)
* [query parameters](examples/std/query_parameters.go)
* [bind params](examples/std/bind.go) (deprecated in favour of native query parameters)
* [client info](examples/std/client_info.go)
* [multi-host / failover](examples/std/multi_host.go)
* [bfloat16](examples/std/bfloat16.go)
* [dynamic](examples/std/dynamic.go)
* [variant](examples/std/variant.go)
* [qbit](examples/std/qbit.go)
* [geo](examples/std/geo.go)
* [ephemeral columns (native)](examples/std/ephemeral_native.go)
* [ephemeral columns (http)](examples/std/ephemeral_http.go)

## Third-party libraries

* [clickhouse-go-rows-utils](https://github.com/EpicStep/clickhouse-go-rows-utils) - utilities that simplify working with rows.

## ClickHouse alternatives - ch-go

Versions of this client >=2.3.x utilise [ch-go](https://github.com/ClickHouse/ch-go) for their low level encoding/decoding. This low lever client provides a high performance columnar interface and should be used in performance critical use cases. This client provides more familar row-oriented and `database/sql` semantics at the cost of some performance. See [TYPES.md](TYPES.md) for the full mapping between Go and ClickHouse types.

Both clients are supported by ClickHouse.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for local setup, test commands, and PR guidelines.
Agent and AI assistant instructions live in [.claude/CLAUDE.md](.claude/CLAUDE.md) (also available as `AGENTS.md`).

## Third-party alternatives

* Database client/clients:
	* [mailru/go-clickhouse](https://github.com/mailru/go-clickhouse) (uses the HTTP protocol)
	* [uptrace/go-clickhouse](https://github.com/uptrace/go-clickhouse) (uses the native TCP protocol with `database/sql`-like API)
	* Drivers with columnar interface:
		* [vahid-sohrabloo/chconn](https://github.com/vahid-sohrabloo/chconn)

* Insert collectors:
	* [KittenHouse](https://github.com/YuriyNasretdinov/kittenhouse)
	* [nikepan/clickhouse-bulk](https://github.com/nikepan/clickhouse-bulk)
