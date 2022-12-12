## 2.4.3, 2022-11-30
### Bug Fixes
* Fix in batch concurrency - batch could panic if used in separate go routines. <br/>
The issue was originally detected due to the use of a batch in a go routine and Abort being called after the connection was released on the batch. This would invalidate the connection which had been subsequently reassigned. <br/>
This issue could occur as soon as the conn is released (this can happen in a number of places e.g. after Send or an Append error), and it potentially returns to the pool for use in another go routine. Subsequent releases could then occur e.g., the user calls Abort mainly but also Send would do it. The result is the connection being closed in the release function while another batch or query potentially used it. <br/>
This release includes a guard to prevent release from being called more than once on a batch. It assumes that batches are not thread-safe - they aren't (only connections are).
## 2.4.2, 2022-11-24
### Bug Fixes
- Don't panic on `Send()` on batch after invalid `Append`. [#830](https://github.com/ClickHouse/clickhouse-go/pull/830)
- Fix JSON issue with `nil` if column order is inconsisent. [#824](https://github.com/ClickHouse/clickhouse-go/pull/824)

## 2.4.1, 2022-11-23
### Bug Fixes
- Patch release to fix "Regression - escape character was not considered when comparing column names". [#828](https://github.com/ClickHouse/clickhouse-go/issues/828)

## 2.4.0, 2022-11-22
### New Features
- Support for Nullables in Tuples. [#821](https://github.com/ClickHouse/clickhouse-go/pull/821) [#817](https://github.com/ClickHouse/clickhouse-go/pull/817)
- Use headers for auth and not url if SSL. [#811](https://github.com/ClickHouse/clickhouse-go/pull/811)
- Support additional headers. [#811](https://github.com/ClickHouse/clickhouse-go/pull/811)
- Support int64 for DateTime. [#807](https://github.com/ClickHouse/clickhouse-go/pull/807)
- Support inserting Enums as int8/int16/int. [#802](https://github.com/ClickHouse/clickhouse-go/pull/802)
- Print error if unsupported server. [#792](https://github.com/ClickHouse/clickhouse-go/pull/792)
- Allow block buffer size to tuned for performance - see `BlockBufferSize`. [#776](https://github.com/ClickHouse/clickhouse-go/pull/776)
- Support custom datetime in Scan. [#767](https://github.com/ClickHouse/clickhouse-go/pull/767)
- Support insertion of an orderedmap. [#763](https://github.com/ClickHouse/clickhouse-go/pull/763)

### Bug Fixes
- Decompress errors over HTTP. [#792](https://github.com/ClickHouse/clickhouse-go/pull/792)
- Use `timezone` vs `timeZone` so we work on older versions. [#781](https://github.com/ClickHouse/clickhouse-go/pull/781)
- Ensure only columns specified in INSERT are required in batch. [#790](https://github.com/ClickHouse/clickhouse-go/pull/790)
- Respect order of columns in insert for batch. [#790](https://github.com/ClickHouse/clickhouse-go/pull/790)
- Handle double pointers for Nullable columns when batch inserting. [#774](https://github.com/ClickHouse/clickhouse-go/pull/774)
- Use nil for `LowCardinality(Nullable(X))`. [#768](https://github.com/ClickHouse/clickhouse-go/pull/768)

### Breaking Changes
- Align timezone handling with spec. [#776](https://github.com/ClickHouse/clickhouse-go/pull/766), specifically:
    - If parsing strings for datetime, datetime64 or dates we assume the locale is Local (i.e. the client) if not specified in the string.
    - The server (or column tz) is used for datetime and datetime64 rendering. For date/date32, these have no tz info in the server. For now, they will be rendered as UTC - consistent with the clickhouse-client
    - Addresses bind when no location is set
