## 2.8.3, 2023-04-03

### Bug fixes

- Revert: Expire idle connections no longer acquired during lifetime [#958](https://github.com/ClickHouse/clickhouse-go/pull/958) by @jkaflik

## 2.8.2, 2023-03-31

### Bug fixes

- Expire idle connections no longer acquired during lifetime [#945](https://github.com/ClickHouse/clickhouse-go/pull/945) by @jkaflik

## 2.8.1, 2023-03-29

### Bug fixes

- Fix idle connection check for TLS connections [#951](https://github.com/ClickHouse/clickhouse-go/pull/951) by @jkaflik & @alekar

## 2.8.0, 2023-03-27

### New features

- Support customized "url path" in http connection [#938](https://github.com/ClickHouse/clickhouse-go/pull/938) by @crisismaple
- Allow Auth.Database option to be empty [#926](https://github.com/ClickHouse/clickhouse-go/pull/938) by @v4run

### Chores

- Bump github.com/stretchr/testify from 1.8.1 to 1.8.2 [#933](https://github.com/ClickHouse/clickhouse-go/pull/933)
- fix: small typo in the text of an error [#936](https://github.com/ClickHouse/clickhouse-go/pull/936) by @lspgn
- Improved bug template [#916](https://github.com/ClickHouse/clickhouse-go/pull/916) by @mshustov

## 2.7.0, 2023-03-08

### New features

- Date type with user location [#923](https://github.com/ClickHouse/clickhouse-go/pull/923) by @jkaflik
- Add AppendRow function to BatchColumn [#927](https://github.com/ClickHouse/clickhouse-go/pull/927) by @pikot

### Bug fixes

- fix: fix connect.compression's format verb [#924](https://github.com/ClickHouse/clickhouse-go/pull/924) by @mind1949
- Add extra padding for strings shorter than FixedColumn length [#910](https://github.com/ClickHouse/clickhouse-go/pull/910) by @jkaflik

### Chore

- Bump github.com/andybalholm/brotli from 1.0.4 to 1.0.5 [#911](https://github.com/ClickHouse/clickhouse-go/pull/911)
- Bump github.com/paulmach/orb from 0.8.0 to 0.9.0 [#912](https://github.com/ClickHouse/clickhouse-go/pull/912)
- Bump golang.org/x/net from 0.0.0-20220722155237-a158d28d115b to 0.7.0 [#928](https://github.com/ClickHouse/clickhouse-go/pull/928)

## 2.6.5, 2023-02-28

### Bug fixes

- Fix array parameter formatting in binding mechanism [#921](https://github.com/ClickHouse/clickhouse-go/pull/921) by @genzgd

## 2.6.4, 2023-02-23

### Bug fixes

- Fixed concurrency issue in stdConnOpener [#918](https://github.com/ClickHouse/clickhouse-go/pull/918) by @jkaflik

## 2.6.3, 2023-02-22

### Bug fixes

- Fixed `lib/binary/string_safe.go` for non 64bit arch [#914](https://github.com/ClickHouse/clickhouse-go/pull/914) by @atoulme
 
## 2.6.2, 2023-02-20

### Bug fixes

- Fix decimal encoding with non-standard exponential representation [#909](https://github.com/ClickHouse/clickhouse-go/pull/909) by @vogrelord
- Add extra padding for strings shorter than FixedColumn length [#910](https://github.com/ClickHouse/clickhouse-go/pull/910) by @jkaflik

### Chore

- Remove Yandex ClickHouse image from Makefile [#895](https://github.com/ClickHouse/clickhouse-go/pull/895) by @alexey-milovidov
- Remove duplicate of error handling [#898](https://github.com/ClickHouse/clickhouse-go/pull/898) by @Astemirdum
- Bump github.com/ClickHouse/ch-go from 0.51.2 to 0.52.1 [#901](https://github.com/ClickHouse/clickhouse-go/pull/901)

## 2.6.1, 2023-02-13

### Bug fixes

- Do not reuse expired connections (`ConnMaxLifetime`) [#892](https://github.com/ClickHouse/clickhouse-go/pull/892) by @iamluc
- Extend default dial timeout value to 30s [#893](https://github.com/ClickHouse/clickhouse-go/pull/893) by @jkaflik
- Compression name fixed in sendQuery log  [#884](https://github.com/ClickHouse/clickhouse-go/pull/884) by @fredngr

## 2.6.0, 2023-01-27

### New features

- Client info specification implementation [#876](https://github.com/ClickHouse/clickhouse-go/pull/876) by @jkaflik

### Bug fixes

- Better handling for broken connection errors in the std interface [#879](https://github.com/ClickHouse/clickhouse-go/pull/879) by @n-oden

### Chore

- Document way to provide table or database identifier with query parameters [#875](https://github.com/ClickHouse/clickhouse-go/pull/875) by @jkaflik
- Bump github.com/ClickHouse/ch-go from 0.51.0 to 0.51.2 [#881](https://github.com/ClickHouse/clickhouse-go/pull/881)

## 2.5.1, 2023-01-10

### Bug fixes

- Flag connection as closed on broken pipe [#871](https://github.com/ClickHouse/clickhouse-go/pull/871) by @n-oden

## 2.5.0, 2023-01-10

### New features

- Buffered compression column by column for a native protocol. Introduces the `MaxCompressionBuffer` option - max size (bytes) of compression buffer during column-by-column compression (default 10MiB) [#808](https://github.com/ClickHouse/clickhouse-go/pull/808) by @gingerwizard and @jkaflik
- Support custom types that implement `sql.Scanner` interface (e.g. `type customString string`) [#850](https://github.com/ClickHouse/clickhouse-go/pull/850) by @DarkDrim
- Append query options to the context instead of overwriting [#860](https://github.com/ClickHouse/clickhouse-go/pull/860) by @aaron276h
- Query parameters support [#854](https://github.com/ClickHouse/clickhouse-go/pull/854) by @jkaflik
- Expose `DialStrategy` function to the user for custom connection routing. [#855](https://github.com/ClickHouse/clickhouse-go/pull/855) by @jkaflik

### Bug fixes

- Close connection on `Cancel`. This is to make sure context timed out/canceled connection is not reused further [#764](https://github.com/ClickHouse/clickhouse-go/pull/764) by @gingerwizard
- Fully parse `secure` and `skip_verify` in DSN query parameters. [#862](https://github.com/ClickHouse/clickhouse-go/pull/862) by @n-oden

### Chore

- Added tests covering read-only user queries [#837](https://github.com/ClickHouse/clickhouse-go/pull/837) by @jkaflik
- Agreed on a batch append fail semantics [#853](https://github.com/ClickHouse/clickhouse-go/pull/853) by @jkaflik

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
