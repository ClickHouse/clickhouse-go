# Arbitrary Input/Output Format Support in clickhouse-go — Prototype Plan

## Context

clickhouse-go today hardcodes the Native wire format everywhere:
- HTTP: `conn_http.go:201` forces `default_format=Native`, and `createRequest` (`conn_http.go:640`) blocks users from overriding it via settings.
- Both protocols: `batch.go:10-24` regex-strips any user `FORMAT` clause from INSERTs and forces `FORMAT Native`.

But the two protocols differ fundamentally (confirmed against the ClickHouse C++ codebase):
- **HTTP**: the *server* formats both directions — `executeQuery.cpp` picks the output format from the query/`default_format` and streams formatted bytes; INSERT bodies can be in any declared format. All ~70 formats come for free; the client is a byte pipe.
- **TCP native**: the server *always* exchanges Native blocks (`TCPHandler.cpp` uses `NativeWriter` unconditionally). `clickhouse-client` does all conversion client-side via `IOutputFormat`/`IInputFormat` (`src/Client/ClientBase.cpp:808-868, 2217-2253`).

**Goal (user-confirmed scope):** a hybrid prototype — new raw-stream methods on `driver.Conn` for both SELECT and INSERT; HTTP delegates formatting to the server; TCP uses a pluggable client-side codec registry with built-in codecs: CSV, JSONEachRow, **Parquet** (customer-driven priority), plus ArrowStream as a near-free byproduct of the Parquet implementation. Clear error for unregistered formats.

Note: Parquet over **HTTP** works with zero codec code (server formats both directions) — that path lands first and is likely the immediate customer solution; the TCP Parquet codec is the client-side equivalent.

## Public API

Add to `driver.Conn` in `lib/driver/driver.go` (compile-breaks external mocks of the interface — accepted, call out in PR):

```go
// QueryFormat executes query and returns the result encoded in the given
// ClickHouse format (e.g. "CSV", "JSONEachRow") as a raw byte stream.
// Caller must Close the stream; until then it holds a connection (visible in Stats).
// Put the format in the argument, not as a FORMAT clause in the query.
QueryFormat(ctx context.Context, format string, query string, args ...any) (io.ReadCloser, error)

// InsertFormat executes the INSERT query, streaming data (pre-encoded in
// format) as the payload. Any FORMAT/VALUES suffix in query is replaced; the format
// argument is authoritative. Returns once the server commits or rejects the insert.
InsertFormat(ctx context.Context, format string, query string, data io.Reader) error
```

`io.Reader` (not `io.WriteCloser`) for insert: one synchronous call, one error path, nothing to forget to close, maps 1:1 to HTTP request body and files/pipes. Callers wanting a writer use `io.Pipe()`.

Naming: decided — `QueryFormat` / `InsertFormat`.

## Codec package — new `lib/format`

Small factory-style interfaces (state lives in per-stream Encoder/Decoder; `Codec` is stateless/concurrent-safe):

```go
type Codec interface {
    Name() string                   // canonical, case-sensitive: "CSV"
    NewEncoder(w io.Writer) Encoder // SELECT: blocks -> bytes
    NewDecoder(r io.Reader) Decoder // INSERT: bytes -> blocks
}
type Encoder interface {
    WriteBlock(block *proto.Block) error // must accept zero-row (schema-only) blocks
    Close() error                        // trailer + flush; does not close w
}
type Decoder interface {
    // Appends up to maxRows rows to block (columns pre-built from server schema);
    // returns rows appended; io.EOF (possibly with rows) at end of input.
    ReadBlock(block *proto.Block, maxRows int) (int, error)
}
```

**Registry — no globals:** `Options.FormatCodecs []format.Codec` in `clickhouse_options.go`; `setDefaults()` builds an unexported `map[string]format.Codec` seeded with built-in CSV, JSONEachRow, Parquet, and ArrowStream, overlaid by user entries (override by `Name()`). Lookup helper `(o *Options).formatCodec(name)`. Third parties implement `format.Codec` and pass it in Options.

### Built-in codecs
- `lib/format/text.go`: shared `renderText(col column.Interface, row int)` (via `col.Row(row, false)` + strconv/time formatting: `2006-01-02 15:04:05` for DateTime, `2006-01-02` for Date) and `appendText(col, s)` inverse (switch on `col.ScanType().Kind()`; numeric columns require explicit parsing — `AppendRow(string)` does not convert; Nullable NULL → `AppendRow(nil)`).
- `lib/format/csv.go`: `encoding/csv` (RFC4180 quoting matches server default), NULL as `\N`, row-numbered decode errors.
- `lib/format/jsoneachrow.go`: encoder writes `{"col":<json>,...}\n` preserving column order; decoder via `bufio.Scanner` → `map[string]json.RawMessage` → `reflect.New(col.ScanType())` + `json.Unmarshal` + `AppendRow`.
- Complex types (Array/Map/Tuple/JSON) on decode → clear "not supported by client-side codec; use HTTP protocol" error, never silent corruption. Fidelity caveats (float rendering, DateTime64 precision) documented in package doc.

### Parquet + ArrowStream codecs (arrow-go based)

New dependency: `github.com/apache/arrow-go/v18` (official Apache implementation; bundles Arrow, Parquet, and the `pqarrow` bridge — same architecture as ClickHouse server, which implements Parquet via the Arrow C++ library). Prototype adds it to the main module; **flag for productionization**: consider a nested Go module (e.g. `format/parquet/go.mod`) so core users don't pull arrow-go — the `Options.FormatCodecs` registry is exactly what makes that split possible later with zero API change.

- `lib/format/arrowconv.go`: the one nontrivial piece — `proto.Block ↔ arrow.Record` converter, written once and shared by both codecs:
  - `blockToRecord(block *proto.Block, pool memory.Allocator) (arrow.Record, error)` — map columns via `col.Type()`/`col.Row(i, false)` into arrow builders.
  - `appendRecordToBlock(rec arrow.Record, block *proto.Block) error` — inverse for INSERT.
  - Type mapping (prototype subset): Int/UInt8-64 ↔ arrow ints/uints, Float32/64, String/FixedString ↔ utf8/binary, Date/Date32 ↔ date32, DateTime ↔ timestamp[s], DateTime64 ↔ timestamp by scale (3→ms, 6→us, 9→ns), Nullable ↔ nullable field, Array(T) ↔ list<T> if time permits. Anything else → clear "type X not supported by client-side Parquet/Arrow codec; use HTTP protocol" error.
- `lib/format/parquet.go`: `Parquet` codec.
  - Encoder: buffer rows per block → `pqarrow.NewFileWriter`; one row group per `WriteBlock` (or coalesce small blocks); `Close()` writes the Parquet footer. Streams fine — footer last.
  - Decoder: **Parquet cannot be decoded from a pure io.Reader** (metadata footer at end of file; readers need io.ReaderAt + size). `NewDecoder` slurps the input into memory (`bytes.Reader`), then `pqarrow.ReadTable` → records → `ReadBlock` slices up to maxRows. Documented limitation: TCP Parquet INSERT buffers the whole payload in memory; for very large files use HTTP (server-side, no client buffering).
- `lib/format/arrowstream.go`: `ArrowStream` codec — proves the design generalizes. ~50 lines on top of arrowconv: `ipc.NewWriter(w, ipc.WithSchema(...))` / `ipc.NewReader(r)`. Fully streaming both directions (Arrow IPC stream format has no trailing footer). Registered as a built-in alongside the rest.

## Dispatch — `clickhouse.go`

Add to `nativeTransport` interface (`clickhouse.go:84`):
```go
queryFormat(ctx, release nativeTransportRelease, format, query string, args ...any) (io.ReadCloser, error)
insertFormat(ctx, release nativeTransportRelease, format, query string, data io.Reader) error
```
Root methods in new `arbitrary_format.go` mirror `Query` (clickhouse.go:148) / `Exec` (clickhouse.go:169): acquire → delegate with `ch.release`; transport owns releasing.

## batch.go refactor

Split `extractNormalizedInsertQueryAndColumns` (`batch.go:14`): new `extractInsertQueryComponents(query) (insertStmt, tableName string, columns []string, err error)` doing everything except the final `" FORMAT Native"` append; existing function becomes a wrapper. HTTP insert path builds `insertStmt + " FORMAT " + format` — the format argument always wins over any user FORMAT clause, same on both transports, reusing the battle-tested regexes.

## HTTP paths — new `conn_http_arbitrary_format.go`

**SELECT** (modeled on `httpConnect.query`, conn_http_query.go:32):
1. `bindQueryOrAppendParameters` as today.
2. Build request via `prepareRequest`, then override the format after the fact: `q := req.URL.Query(); q.Set("default_format", format)` — avoids touching `createRequest`'s deliberate `default_format` skip, and avoids fragile SQL-appending of `FORMAT X` (semicolons/comments). If the user's query carries its own FORMAT clause it wins on HTTP — documented ("format goes in the argument").
3. Compression: gzip/deflate/br via existing `Accept-Encoding` + `compressionPool` decompressing reader — user sees decompressed format bytes. Do NOT set native LZ4/ZSTD `compress=1` (that wraps the body in ClickHouse block framing, defeating raw bytes); skip with doc note.
4. Mid-stream exceptions: wrap the stream in an `exceptionScanReader` — rolling-window scan for `\r\n__exception__\r\n`, on match buffer remainder (cap ~32KiB) and surface `parseExceptionFromBytes` (`conn_http.go:506`) as the next Read error. Contract: "Read may return partial data then an error; for all-or-nothing set `wait_end_of_query=1` via WithSettings" (see tests/http_exception_test.go:24).
5. Return `httpFormatStream` (io.ReadCloser): `Close` = `discardAndClose(res.Body)` + return compression reader to pool + `release(h, err)` exactly once.

**INSERT** (modeled on `httpBatch.Send`, conn_http_batch.go:222, minus block encoding — no DESCRIBE TABLE needed):
1. `options.settings["query"] = insertStmt + " FORMAT " + format`; `Content-Type: application/octet-stream`.
2. gzip/deflate/br: compress user's reader through `io.Pipe` + `compressionPool` writer goroutine (same shape as conn_http_batch.go:249-263; goroutine exits on reader exhaustion or pipe close). LZ4/ZSTD: skip, send plain.
3. `sendStreamQuery` (`conn_http.go:566`); non-200 from `executeRequest` carries the server's parse/insert error. `discardAndClose`, `release`.

## TCP paths — new `conn_arbitrary_format.go`

**SELECT** (modeled on `connect.query`, conn_query.go:10):
- Codec lookup first; if missing → actionable error: `format %q has no client-side codec for the native protocol; register one via Options.FormatCodecs or use Protocol: clickhouse.HTTP`.
- Send query unmodified (server streams Native regardless of FORMAT clause; clickhouse-client does the same). `sendQuery` → `firstBlock`.
- `pr, pw := io.Pipe()`; encoder writes to `pw`. Goroutine (owns the connection; exits on ServerEndOfStream, protocol/encode error, or ctx cancellation via `process`):
  - `enc.WriteBlock(first)` (schema-only block → lets `*WithNames` formats emit headers).
  - `onProcess.data = func(b) { encErr = enc.WriteBlock(b) }` (skip further writes after first error; skip Totals/Extremes packets).
  - After `c.process(ctx, onProcess)`: `enc.Close()` for trailer; `io.ErrClosedPipe` from early reader Close is not an error (keep draining so the conn releases healthy — same semantics as `rows.Close()`); `pw.CloseWithError(errors.Join(procErr, encErr))`; `release(c, procErr)`.
- Contract identical to `Rows`: stream holds the connection until fully read or closed; early `Close` drains; hard abort = cancel ctx (kills socket via `c.cancel`).
- Edge: `firstBlock` returns io.EOF → encode nothing, close encoder, return `io.NopCloser` over any trailer, release immediately.

**INSERT** (synchronous, single-goroutine; reuses prepareBatch handshake, conn_batch.go:22-49):
- `extractNormalizedInsertQueryAndColumns` → `... FORMAT Native`; `sendQuery`; `firstBlock` = server sample block (schema); `block.SortColumns(queryColumns)`.
- Loop: `block.Reset()`; `dec.ReadBlock(block, 65_409 /* ClickHouse DEFAULT_BLOCK_SIZE */)`; if n>0 `c.sendData(block, "")`; break on io.EOF; on decode error `c.cancel()` (aborts server-side INSERT), release, return row-numbered error.
- Finish: `sendData(proto.NewBlock(), "")` end-of-data marker; `c.process(ctx, onProcess)` waits for EndOfStream/exception; release.

## std database/sql — out of scope

Note in PR: could later hook via `sql.Conn.Raw` type assertion; nothing here blocks it (methods live on `nativeTransport`, which std shares).

## Files

**Create:** `lib/format/{format,text,csv,jsoneachrow,arrowconv,parquet,arrowstream}.go` + unit tests (round-trip on hand-built `proto.Block`s, no server; Parquet test additionally validates output with the parquet library's own reader); `arbitrary_format.go` (root dispatch); `conn_arbitrary_format.go` (TCP); `conn_http_arbitrary_format.go` (HTTP + exceptionScanReader + httpFormatStream); `tests/arbitrary_format_test.go`; `examples/clickhouse_api/arbitrary_format.go` (repo convention).

**Modify:** `lib/driver/driver.go` (2 Conn methods + io import); `clickhouse.go` (nativeTransport + dispatch); `clickhouse_options.go` (FormatCodecs + map + lookup); `batch.go` (split extractInsertQueryComponents); `go.mod` (+`github.com/apache/arrow-go/v18`).

## Implementation order

0. Commit this design doc into the repo as `design/support-arbitrary-format.md` (first commit on the branch).
1. `lib/format` package: interfaces + text codecs (CSV, JSONEachRow) + unit tests (self-contained).
2. `Options.FormatCodecs` + lookup; `batch.go` refactor (existing batch_test.go guards the regex).
3. `driver.Conn` + `nativeTransport` + root dispatch.
4. HTTP select + insert (pure passthrough — fastest validation; **Parquet works here immediately**, the customer path).
5. TCP select (io.Pipe) + TCP insert, validated with the text codecs.
6. arrow-go dep + `arrowconv.go` converter + Parquet codec + ArrowStream codec + unit tests.
7. Integration tests, example, CHANGELOG.

## Verification

Unit: `go vet ./...`, `go test ./lib/format/...`.

Integration (`tests/arbitrary_format_test.go`, via existing `tests/utils.go` helpers `GetConnectionTCP`/`GetConnectionHTTP` + docker-compose per CONTRIBUTING.md; process-unique table names — Cloud CI shares one service):
- HTTP SELECT CSV + JSONEachRow: seed via normal batch, assert exact server-rendered bytes.
- HTTP SELECT/INSERT **Parquet**: round-trip — `QueryFormat("Parquet", ...)`, parse the stream with arrow-go's parquet reader, assert values; then feed those bytes back via `InsertFormat` into a second table and verify via native Query.
- HTTP INSERT text formats from `strings.Reader`, including an INSERT statement containing a stray `FORMAT` clause (must be replaced); verify rows back via **native** Query (std Scan masks bind/format bugs).
- TCP SELECT all four codecs: for text codecs compare client-encoded output vs server-encoded HTTP output for fidelity-safe types (ints, strings, DateTime, Nullable); for Parquet/ArrowStream parse client output with arrow-go readers and assert values.
- TCP INSERT all four codecs: multi-block input (>65,409 rows); malformed-row case asserting row-numbered error and pool stays healthy (`Stats`).
- Cross-protocol Parquet: bytes produced by the TCP client-side codec must load via HTTP `InsertFormat` (server parses them) — proves interop of client-written Parquet with server expectations.
- Unregistered TCP format (e.g. "ORC") → error mentioning FormatCodecs and HTTP.
- Mid-stream HTTP exception: `SELECT throwIf(number=100000) FROM numbers(1000000)` with settings from tests/http_exception_test.go → partial data then parsed exception.
- Cancellation: cancel ctx mid-read on TCP → reader unblocks with error, goroutine exits, pool recovers.
- Compression: gzip HTTP both directions; LZ4-configured connection still works (native compression skipped for these calls).

## Risks / decisions to sign off

1. **`driver.Conn` additions compile-break external mocks** — accepted per chosen direction; fallback is a separate optional interface + accessor.
2. **HTTP mid-stream exception detection is best-effort** (`__exception__` marker scan; server-version dependent, theoretical payload false-positive). Promise = "partial bytes then error"; strict atomicity requires `wait_end_of_query=1`. Sharpest correctness edge.
3. **TCP codec fidelity ≠ server output** (floats, DateTime64 precision, CSV `\N` ambiguity, complex types unsupported) — prototype-accepted, documented.
4. **FORMAT-clause precedence asymmetry on SELECT**: in-query clause beats the argument on HTTP, ignored on TCP. Mitigation: docs ("format goes in the argument"); rejecting such queries is regex-fragile, not for prototype.
5. **arrow-go is a heavy dependency** added to the core module for the prototype. Before merge, decide: keep in core vs nested Go module (`lib/format` registry makes the split painless later). Also: TCP Parquet INSERT buffers the whole payload in memory (footer-at-end constraint of the format itself, not our design) — HTTP is the recommended path for very large Parquet files.
6. **Layering path**: a future Rows-style/scanning API can sit on top of the io.ReadCloser primitive + codec registry — no rework needed. Arrow support demonstrates this: once `blockToRecord` exists, any Arrow-adjacent format (Feather, ORC via arrow bridges) is a thin codec.
