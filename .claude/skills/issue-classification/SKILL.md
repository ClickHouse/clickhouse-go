---
name: issue-classification
description: >
  The classes of an issue in this ClickHouse client repository: the impacts
  (Security, Data integrity, Stability, Performance) and the area (Connectivity,
  Data types & formats, Queries, Configuration, API & diagnostics, Environment,
  Docs & project, Other), and the native classes (format, protocol) of an issue
  that needs a change in the client's code for the Native format or the native
  TCP protocol. Use this skill when the task is to "classify an
  issue" or "give the classes of an issue", and when you triage, file, or
  prioritize an issue: the issue-priority skill uses the impacts.
---

# Issue classification

The classes name **what kind of problem an issue is about**, with the same words in every
ClickHouse client repo. They are an input to the priority, and they make issue counts
comparable across clients.

An issue has:

- **Impacts** — what goes wrong: none, one, or more of the four impact classes.
- **One area** — the part of the client whose behavior is wrong or missing.
- **Native classes** — none, one, or both of format and protocol: the issue needs a change in
  the client's code for the Native format or for the native TCP protocol.

The classes do not replace the kind of request (bug, feature, question) or the reach tier of
a repo's priority rules.

Every ClickHouse client repository has a copy of this file, so that any agent can classify an
issue in the repository. Keep the copies the same where you can, so that the classes mean the
same in every client. Put a change that applies only to this repository in the section "This
repository" at the end.

## Rules

1. Give each impact that applies, one area, and each native class that applies.
2. Use only what the issue, a reproduction, or code inspection of the current code
   **shows**. Do not give a class for an effect
   that is only possible. Classes can raise the priority, so each class needs evidence.
3. Classify by the **observed effect**, not by the location of the code. Example: a
   connection-string defect that makes the client drop rows with no error has the impact
   Data integrity. The native classes are the exception: they follow the code (section
   "Native classes").
4. Data integrity and Stability are for defects only. Security and Performance are also for
   feature requests: a new authentication method, a request to make an operation faster.
   The priority rules rate such requests as feature requests.
5. An issue with no harm has no impact. This is the normal case.

## Impact classes

| Impact | Use when |
|---|---|
| **Security** | Two parts. A **vulnerability**: a defect makes the protection weaker: it exposes credentials or data (also a secret that leaks to a log, an error message, a URL, or another host), lets a value change the meaning of a query (injection), skips or weakens a TLS or identity check, corrupts memory (an out-of-bounds write, a use after free), or lets input that the application takes from its users stop the client; also a vulnerability in a dependency. The boundaries "Hostile server or network" and "Dependencies" below say when these count. **Authentication**: an authentication method that does not work or is missing, or how the client takes credentials (for example a token provider or a credentials file); a secret that leaks is a vulnerability, not authentication. Say which part applies: the priority rules of a repo rate the two parts differently. |
| **Data integrity** | Data that the caller wrote, or a result that the caller reads, is missing, incomplete, or different from the value given or stored, and the client reports no error. This includes a failed operation that the client reports as a success, a server error that the client ignores, overflow, lost precision, a time zone shift, a wrong decode, a stream that loses synchronization, a duplicate write, and a write to the wrong table. |
| **Stability** | The process stops in a way that the caller cannot handle as a normal error (abort, segmentation fault, unrecovered panic, stack overflow), a call does not return (deadlock, hang), or a resource leaks until none is left (connections, sockets, memory). Also a thread-safety defect. |
| **Performance** | The result is correct, but the operation is too slow, or uses too much memory or CPU. Also a request to make an operation faster. |

Boundaries:

- **Data integrity needs "no error".** When the client rejects a value with an error, the
  issue has no Data integrity impact. When the caller gets an error, but the error is wrong
  or not clear, the issue has no Data integrity impact either: it has the area API &
  diagnostics.
- **Security and values that are not quoted.** A value or an identifier that the client
  does not quote or escape gets Security only when the issue shows a path from untrusted
  input. A string value that leaves its literal because the client does not escape it is
  such a path. An identifier that the client does not quote, with no such path, is the area
  Queries with no impact.
- **TLS.** A TLS check that the client skips or weakens is Security. A connection with TLS
  that fails has no impact; its area is Connectivity.
- **Memory.** High memory use without a failure is Performance, not Stability.
- **Hostile server or network.** A hang, or a crash with no memory corruption, that only a
  hostile server or network causes is Stability, not Security. Memory corruption is a
  vulnerability also when only a hostile server or network causes it. A crash while the
  client reads data from the server (for example an access violation or `SIGSEGV`) can be
  memory corruption: rule that out by code inspection before you classify it as Stability.
- **Dependencies.** A vulnerability in a dependency is Security (vulnerability) when the
  client calls the vulnerable code, and also when it is not published yet. A published one
  (CVE or GHSA) in code that the client does not call has no impact: the issue asks for a
  dependency update (area Environment). Report one that is not published yet also to the
  maintainers of that dependency, in private, through their security policy, never in a
  public issue.
- **An error is not Stability.** A normal exception or error value is not a crash. In Go
  and Rust, a panic that the library does not recover is Stability. In Java, a
  `StackOverflowError`, `OutOfMemoryError`, or `VerifyError` is Stability.

## A vulnerability that is not public yet

A security issue must not stay public with a description of how to reproduce it. An issue
is a **confirmed vulnerability** when a run or code inspection shows one of these:

- credentials or data are exposed to a party that must not get them (also a secret that leaks);
- a value changes the meaning of a query (injection);
- a TLS or identity check is skipped or weakened;
- memory corruption (see "Hostile server or network" in the boundaries above);
- a vulnerability in a dependency that is not published yet (see "Dependencies" above);
- input that the application takes from its users stops the client.

Never put a confirmed vulnerability into a public comment, issue, pull request, or
Classification line. Report it in a private GitHub vulnerability report for this repository
(**Security** → **Report a vulnerability**). The issue-priority skill of this repository says
how to handle the public issue and how to rate the vulnerability. When you can neither confirm
nor rule out a vulnerability, do not discuss it in public either: ask a maintainer in private,
or send a private report that says what is not confirmed.

These are not affected: a vulnerability that is already public (a published CVE or GHSA, for
example in a dependency), and the part authentication (a method that does not work or is
missing). Classify them as usual.

## Area classes

| Area | Scope |
|---|---|
| **Connectivity** | Connect and handshake, a connection with TLS, proxies and redirects, the connection pool, keep-alive, reconnect and failover, retries, timeouts, cancellation, the transport (HTTP, native TCP), compression on the wire. |
| **Data types & formats** | Parsing of type names, encoding and decoding of values, the mapping to language types and to rows or objects (POJO, serde, struct), data formats (Native, RowBinary, JSON, Parquet, Arrow), support for a new type or format. |
| **Queries** | How the client builds a statement: parameters and placeholders, quoting and escaping, SQL parsing in the client, size limits of the statement and its parameters. How the client runs a statement: batches and streaming inserts, sessions and roles, external data, totals, asynchronous inserts, the queries that the client sends on its own (`DESCRIBE`, server information), caches of table structure. |
| **Configuration** | Reading and passing options: connection strings, DSNs, URLs, and option objects (parsing, validation, defaults); ClickHouse settings for a connection or a query; settings that the client sends on its own. |
| **API & diagnostics** | The shape of the public API (a missing method or overload, a wrong type signature), and conformance to a standard interface that the client implements (JDBC, ODBC, ADO.NET, `database/sql`, DB-API, SQLAlchemy, R2DBC), including metadata and catalog functions. Errors that are wrong, incomplete, or not clear. Logging, metrics, tracing, progress, the query ID. |
| **Environment** | A build from sources, support for a compiler, runtime, operating system, or architecture, package managers, released artifacts, installers, dependencies. Also a failure with one ClickHouse server version only, or with one third-party tool only (Power BI, Tableau, Spark, DBeaver). |
| **Docs & project** | Documentation, examples, or the changelog are wrong or missing. Tests, CI, the release process, refactoring. |
| **Other** | No area fits. |

Boundaries:

- **Configuration or the area that an option controls.** If the client reads an option
  correctly but the behavior that the option controls is wrong, use the area of that
  behavior. Example: a keep-alive option that has no effect is Connectivity.
- **Authentication.** An authentication issue has the impact Security and the area
  Connectivity.
- **Environment or the area of the failure.** Use Environment when the same call works in
  general, and fails only with one server version, one platform, or one third-party tool.
  A failure with all server versions and without the tool has the area of the failure.
- **API & diagnostics or the area of the failure.** When the problem is the quality of an
  error, use API & diagnostics. When the problem is that the operation fails at all, use
  the area of the operation.

Rules for **Other**:

- Other is the last choice. Say why no area fits.
- Other does not raise the priority.
- A subject that repeats in Other is a sign that a class is missing: report it.

## Native classes

The native classes mark an issue that needs a change in the client's **native code**: its
codec for the ClickHouse `Native` format, or its implementation of the native TCP protocol. A
shared core for this code is planned (`ClickHouse/ch-core-rs`). The native classes count how
much maintenance this code needs in each client.

| Native class | Code |
|---|---|
| **format** | Reading and writing data in the `Native` format, over HTTP (`FORMAT Native`) and in the Data packets of the native protocol: the block structure (column and row counts, column names, type names), the parsing of the type names of a block, and the bytes of each column type: fixed widths, null maps, offsets, LowCardinality dictionaries and keys, serialization versions and kinds (also sparse columns), Variant and Dynamic discriminators, JSON paths. |
| **protocol** | The native TCP protocol: the packets and their fields (Hello and its addendum, Query with the client information, the settings, and the parameters, Data, Exception, Progress, ProfileInfo, ProfileEvents, Totals, Extremes, Log, TableColumns, Ping, Cancel, EndOfStream), the negotiation of the protocol revision and the features that the revision controls, the order of the packets, the framing of compressed blocks (checksum, method, sizes), and external tables in the protocol. |

Rules:

1. Classify by the **code**, not by the observed effect (an exception to rule 3 of "Rules").
   Give a native class when a run, code inspection, or the fix shows that the cause is in that
   code. A feature request gets a native class when the feature changes that code, for
   example support for the native protocol, for a new type in the Native format, or for a new
   packet.
2. The code around the native code gets no native class: the conversion between a decoded
   value and a language type (a scan into a struct or an object, a time type of the
   language), the connection pool, TLS, the socket, timeouts and retries, the public API, and
   the other formats (RowBinary, JSON, Arrow, Parquet, ODBCDriver2). A failure that occurs
   only with the native protocol, but that has its cause in this code, gets no native class.
3. Code that the client uses for the Native format and for another format (for example one
   parser of type names) gets the class format when the defect also occurs on the Native
   path.
4. When the evidence does not show where the cause is, give no native class.
5. The native classes do not change the impacts, the area, or the priority. A format issue
   usually has the area Data types & formats. A protocol issue usually has the area
   Connectivity, Queries, or API & diagnostics. A failure with one server version only has
   the area Environment.
6. A client that has no native code gets a native class only for a request to add it.

On GitHub, the label `native-format` shows the class format, and the label `native-protocol`
shows the class protocol.

| Issue | Classification |
|---|---|
| A `LowCardinality(String)` column is written with wrong dictionary keys | `impact: Data integrity · area: Data types & formats (LowCardinality(String)) · native: format` |
| The client cannot read a new serialization version of `JSON` that one new server version sends | `impact: none · area: Environment (JSON, ClickHouse 26.x) · native: format` |
| The client sends the client version 0.0.0 in the Hello packet, and one server version rejects it | `impact: none · area: Environment (client version, ClickHouse 26.10) · native: protocol` |
| The client allocates a new buffer for each compressed block | `impact: Performance · area: Connectivity (compression) · native: protocol` |
| A request to support the native TCP protocol | `impact: none · area: Connectivity (native protocol) · native: protocol` |
| A scan of a `DateTime64` column into a time type of the language loses the time zone | `impact: Data integrity · area: Data types & formats (DateTime64)`: the conversion to a language type is not native code |
| The connection pool leaks connections that use the native protocol | `impact: Stability · area: Connectivity (connection pool)` |

## Examples

| Issue | Impacts | Area |
|---|---|---|
| A bare `Enum(...)` type hint is written as `Enum8`, which corrupts the data | Data integrity | Data types & formats |
| A flush returns no error when the server rejects the INSERT | Data integrity | Queries |
| The connection pool leaks connections until none is left | Stability | Connectivity |
| String parameters are not escaped | Security (vulnerability) | Queries |
| `LowCardinality(Nullable(String))` cannot be read: the client returns an error | none | Data types & formats |
| A server error loses its code when the response arrives in two chunks | none | API & diagnostics |
| A request for Kerberos authentication | Security (authentication) | Connectivity |
| Each compressed read fails on one new server version | none | Environment |
| Parameter binding takes quadratic time | Performance | Queries |

## How to write the classification

One line, in this form. In a triage comment, the line has the name **Classification**:

`impact: <impacts, separated by a comma, or "none"> · area: <area> (<subject>)`

When a native class applies, add it at the end of the line: ` · native: format`,
` · native: protocol`, or ` · native: format, protocol`. When none applies, add nothing.

For Security, add the part in parentheses: `Security (vulnerability)` or
`Security (authentication)`.

The **subject** names what the issue is about inside the area, in one to four words. Use the
names that ClickHouse or the client API uses:

- **Data types & formats:** always the ClickHouse types or the data format, for example
  `(Enum8, Enum16)`, `(Map)`, `(DateTime64)`, `(RowBinary)`, `(Parquet)`. "Data types &
  formats" alone does not say which type.
- **Other areas:** the feature, for example `(cancellation)`, `(TLS)`, `(connection pool)`,
  `(query parameters)`, `(sessions)`, `(mid-stream errors)`, `(logging)`, `(release process)`.
- With two areas, give a subject after each one.

Examples: `impact: Data integrity · area: Data types & formats (Map)`,
`impact: none · area: Configuration (URL parameters)`,
`impact: Security (vulnerability), Stability · area: Connectivity (TLS)`,
`impact: none · area: Connectivity (ZSTD compression) · native: protocol`.

## This repository

**Native code.** clickhouse-go reads and writes the Native format over TCP and over HTTP
(`default_format=Native`), with the column types of the ch-go `proto` package. Format:
`lib/column/` (`Decode` and `Encode` of each column type, and the type names in `column_gen.go`)
and the blocks (`lib/proto/block.go`, `sendData` and `readData` in `conn.go`, `writeData` and
`readData` in `conn_http.go`). Protocol: the packets of `lib/proto/` (also the quoting of
parameter values in `query.go`), the packet flow of `conn.go`, `conn_handshake.go`,
`conn_send_query.go` (also external tables), `conn_process.go`, `conn_ping.go`, `conn_logs.go`,
and `conn_profile_events.go`, and the compressed blocks of the ch-go `compress` package (over
TCP, and over HTTP with `compress=1`). Not native code: the conversion between column values and
Go types (`Append`, `AppendRow`, `ScanRow`, and `Row` in `lib/column/`, `scan.go`,
`struct_map.go`), the parameter values as text (`bind.go`, `query_parameters.go`), the other
formats over HTTP (`conn_http_format.go`), and the connection pool.
