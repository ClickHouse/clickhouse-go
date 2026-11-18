---
name: issue-priority
description: >
  Decide the priority of a `ClickHouse/clickhouse-go` issue: `Urgent`, `High`,
  `Medium`, or `Low`, for the org issue field **Priority**. Use this skill when
  the task is to "prioritize", "set the priority of", "rate", or "triage" an
  issue, or to explain why an issue has a given priority. It defines the reach
  tiers of the public API, the impact classes, the severity rows, the base-level
  matrix, the modifiers, and the confidence.
---

# Issue priority

This skill is the single source of truth for the **Priority** of
`ClickHouse/clickhouse-go` issues. People and automated triage use the same
rules.

The value goes into the org issue field **Priority** (`Urgent`, `High`,
`Medium`, `Low`). The field is visible to org members only. An automated tool
sets the value once, never changes a value that is already set, and keeps the
level and its reason out of public comments.

## When to set no priority

Set no priority when one of these is true:

- A maintainer can close the issue with no further action: it is already fixed
  on `main`, a duplicate, works as intended, already supported, answered, or
  obsolete.
- Only a deprecated public API reaches the defect (a `Deprecated:` paragraph in
  the Go doc comment, or docs that call the API deprecated; for example
  `Conn.AsyncInsert`, `WithAsync`, `WithStdAsync`, and the `Debug` and `Debugf`
  options). Fixes to deprecated code are not released.
- The issue is a question, or it is about CI, the release process, or a
  discussion.

## Vulnerabilities that are not public yet

A security issue must not stay public with a description of how to reproduce
it. This section applies when a reproduction or code inspection confirms a
vulnerability that is not public yet (impact Security, part vulnerability, in
section 2): a leak of credentials or data, a value that changes the meaning of
a query, a TLS or identity check that is skipped or weakened, memory corruption
(also when only a hostile server or network causes it), or input from the
application's users that stops the client. A hang, or a crash with no memory
corruption, that only a hostile server or network causes is not one: rate it by
the failure, as usual. A crash while the client reads data from the server (for
example an access violation or `SIGSEGV`) can be memory corruption: rule that
out by code inspection before you rate it by the failure.

- Report it in a private GitHub vulnerability report for this repository
  (**Security** → **Report a vulnerability**), never in a public issue,
  comment, or pull request. If a public issue reports it, copy the title, the
  text, and the comments of the issue into the report first.
- Set no Priority on the public issue. Close it with a comment that gives no
  details, and ask a repository administrator to delete it: closing does not
  hide the text of the issue.
- Rate the vulnerability with the rules below, and put the level and its reason
  into the private report as the proposed severity.

A vulnerability that is already public (a published CVE or GHSA, for example in
a dependency), and an authentication method that does not work or is missing,
get a Priority on the issue as usual.

## 1. Reach

**Reach** tells how many applications can hit the defect. It comes from the
public API that leads to the code with the defect: an API that almost every
application calls has a high reach, an API that few applications use has a low
reach. There are four tiers, highest first:

- **Core:** almost every application calls this API.
- **Common:** many applications use it, but not all.
- **Narrow:** few applications use it (rare types, experimental APIs, special
  setups).
- **Satellite:** applications do not import it (examples, tests, docs, tools).

To find the tier, find the most-used public API that reaches the defect, and
look it up in the table below. Use the API, not the file that contains the
defect: a defect in a helper that only `QueryFormat` calls is Narrow, and a
defect in the same helper that `Rows.Scan` also calls is Core.

The native interface (`clickhouse.Open`) and the `database/sql` driver use the same connection and
column code, so a `lib/column/` defect that `Rows.Scan` reaches through either
interface has the tier of that column type. Both interfaces use the native
protocol by default: a defect that only the HTTP protocol shows is Common. The
column codecs and the compression come from ch-go
(`github.com/ClickHouse/ch-go/proto`, `github.com/ClickHouse/ch-go/compress`):
a ch-go defect has the tier of the clickhouse-go path that reaches it.

| Tier      | Public paths |
| --------- | ------------ |
| Core      | the native interface (`clickhouse.Open`, `driver.Conn`: `Query`, `QueryRow`, `Select`, `Exec`, `PrepareBatch` with `Batch.Append` and `Batch.Send`, `Ping`) and the `database/sql` driver (`sql.Open("clickhouse", dsn)`, `clickhouse.OpenDB`, batches with `Begin`, `Prepare`, `Exec`, `Commit`) over the native protocol; the connection layer (dial, connection pool, `DialTimeout`, `ReadTimeout`, context cancellation); username and password authentication, and TLS; `Options` and DSN parsing (`ParseDSN`); error handling (`*clickhouse.Exception`, `*OpError`); query parameters and binding (`bind.go`, `query_parameters.go`, `clickhouse.Named`, `WithParameters`); struct mapping (`Select`, `ScanStruct`, `AppendStruct`); `Rows.Scan` and these column types (`lib/column/`): integers up to 64 bits, `Float32`, `Float64`, `Bool`, `String`, `FixedString`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Decimal`, `UUID`, `Enum8`, `Enum16`, `Nullable`, `LowCardinality`, `Array`, `Map`, `Tuple` |
| Common    | the HTTP protocol (`Protocol: clickhouse.HTTP`, `http://` and `https://` DSNs, `HttpHeaders`); compression (`Compression`: LZ4, LZ4HC, ZSTD, and GZIP, Deflate, Brotli over HTTP); column-wise append (`Batch.Column`); failover and load balancing (more than one address, `ConnOpenStrategy`); the context options `WithSettings`, `WithQueryID`, `WithQuotaKey`, `WithProgress`, `WithLogs`, `WithProfileInfo`, `WithProfileEvents`; async insert through `WithSettings`; the logger (`Logger`); `Conn.Stats`; the column types `JSON`, `IPv4`, `IPv6`, `Int128`, `Int256`, `UInt128`, `UInt256`, `Nested`, `SimpleAggregateFunction` |
| Narrow    | the other column types: Geo (`Point`, `Ring`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`), `Variant`, `Dynamic`, `Time`, `Time64`, `BFloat16`, `QBit`, `Interval`, `Nothing`; external data (`ext`, `WithExternalTable`); raw input and output formats (`QueryFormat`, `InsertFormat`; experimental, HTTP protocol only); JWT authentication (`GetJWT`, `WithJWT`); OpenTelemetry (`WithSpan`); custom dialers (`DialContext`, `DialStrategy`); the HTTP proxy (`HTTPProxyURL`); `ClientInfo`, `WithColumnNamesAndTypes`, `WithBlockBufferSize`, `WithUserLocation` |
| Satellite | `examples/`, `benchmark/`, the test helpers in `tests/`, `docs/`, `README.md`, `TYPES.md`, the agent skills and agents in `.claude/` |

## 2. Severity

First name the **impacts** of the issue. The impact classes are the same in
every ClickHouse client repository. An issue has none, one, or more of them:

| Impact         | Use when |
| -------------- | -------- |
| Security       | a **vulnerability**: a defect makes the protection weaker: it exposes credentials or data, lets a value change the meaning of a query, skips or weakens a TLS or identity check, corrupts memory (an out-of-bounds write, a use after free, also when only a hostile server or network causes it), or lets input that the application takes from its users stop the client (a hang, or a crash with no memory corruption, that only a hostile server or network causes is Stability). Also **authentication**: an authentication method that does not work or is missing |
| Data integrity | data that the caller wrote, or a result that the caller reads, is missing, incomplete, or different from the value given or stored, and the client reports no error; this includes a failed operation that the client reports as a success |
| Stability      | the process stops in a way that the caller cannot handle as a normal error, a call does not return, or a resource leaks until none is left |
| Performance    | the result is correct, but the operation is too slow, or uses too much memory or CPU |

Give an impact only when the issue, a reproduction, or code inspection of the
current code shows it. Most issues have none.

Then use the first severity row that fits. With more than one impact, the
highest row applies:

| Severity                | Use when |
| ----------------------- | -------- |
| Wrong data or security  | impact **Data integrity**: the client returns or stores wrong or incomplete data with no error, or reports success for a query that failed; or impact **Security** with a vulnerability: a security defect |
| Broken, no workaround   | the feature fails (an error, a hang, a panic, a leak that ends in a crash or in exhausted connections), and the caller has no reasonable workaround; also a wrong or incomplete error with no workaround: the failure is reported, but its type, code, or message is wrong or lost. The impact **Stability** (a hang, a panic, such a leak) uses this row when the caller has no reasonable workaround |
| Workaround exists       | the feature fails, but the caller can avoid the failure with a small change in their own code (name the workaround in the reason); also impact **Performance**: a result that is correct but slow or uses too much memory |
| Cosmetic, docs, feature | wrong log or message text, documentation, a feature request |

An issue that has the impact Security with no vulnerability (an authentication
method that does not work or is missing) does not use the first row. Rate an
authentication method that does not work by its failure ("Broken, no
workaround" or "Workaround exists"), and a request for a missing method as a
feature request.

## 3. Base level

| Severity ↓ / Reach →    | Core   | Common | Narrow | Satellite |
| ----------------------- | ------ | ------ | ------ | --------- |
| Wrong data or security  | Urgent | Urgent | High   | Medium    |
| Broken, no workaround   | High   | High   | Medium | Low       |
| Workaround exists       | Medium | Medium | Low    | Low       |
| Cosmetic, docs, feature | Low    | Low    | Low    | Low       |

## 4. Modifiers

Apply them in this order:

1. **Security with a shown path from untrusted input.** The issue or a
   reproduction shows how a value that an application takes from its users
   reaches the defect: `Urgent` at any reach. Skip the other modifiers. A
   defect that puts a caller-supplied value into SQL without quoting or
   escaping (for example in the client-side binding, `bind.go`) is a candidate:
   look for such a path. If none is shown, rate the defect by the failure that
   it shows. A vulnerability that is not public yet goes to a private
   vulnerability report, not to a public issue (see "Vulnerabilities that are
   not public yet").
2. **Panic.** The library panics: a Go `panic` in clickhouse-go or ch-go code,
   not in the caller's code, and not in `examples/`, `benchmark/`, or the
   tests. The level is at least `High`, at any reach. Library code must not
   panic (`AGENTS.md`), and a panic stops the whole process unless the caller
   recovers it.
3. **Ingestion.** The defect is on the insert path (`PrepareBatch`,
   `Batch.Append`, `Batch.AppendStruct`, `Batch.Column`, `INSERT` through
   `database/sql`, async insert), and the client stores data that is lost,
   corrupted, or different from the given value, with no error. This includes
   a missing field, and a column that gets a default value in place of the
   given value, or does not get its `DEFAULT` when the value is not given: up
   one level. This can give `Urgent`. Wrong data in a table stays wrong after
   the fix; a defect on the read path does not change the stored data.
4. **Regression in the latest published release:** up one level. This can give
   `Urgent`. It is a regression only when the latest release shows the problem
   and the release before it does not. The tags are `vX.Y.Z` (for example
   `v2.51.0`).
5. **Customer request in Linear.** The issue links (`linear.app`) a Linear
   issue for a customer request: up one level, but never to `Urgent` (`High`
   stays `High`).

A defect on the query path (`Query`, `QueryRow`, `Select`, `Rows.Scan`) that
does not panic gets neither of rules 2 and 3. So at the same reach and
severity, a query defect ranks below a panic and below an ingestion defect.

## 5. Confidence

A missing reproduction lowers the confidence, not the level. Record the
confidence with the value when the tool that sets it supports it:

| Confidence | Use when |
| ---------- | -------- |
| `HIGH`     | a run shows the problem (or, for a feature, code inspection confirms its absence on `main`) |
| `MEDIUM`   | code inspection shows a defect, but no reproduction was run (for example, it needs ClickHouse Cloud) |
| `LOW`      | the report lacks what a reproduction needs, or a run did not show the problem |

## 6. Reason

Write one line with the value:

`<tier> (<path>) · <impacts, or "no impact"> · <severity> · <modifiers, or "no modifier"> → <level>.`

Name the workaround when the severity is "Workaround exists". When the
confidence is `LOW`, say what is missing.

## 7. Root cause in ch-go

When the root cause is in ch-go (a ch-go-only program shows the problem, or the
defect is in ch-go code), the fix goes into `ClickHouse/ch-go`. It reaches
clickhouse-go users only after a ch-go release and a dependency update in
`go.mod`, so the issue stays open until that update is merged. The priority
stays as the table gives it for the clickhouse-go path.
