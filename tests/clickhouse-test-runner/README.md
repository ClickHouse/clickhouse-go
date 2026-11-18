# clickhouse-test-runner

This harness runs the ClickHouse core SQL tests
([`tests/queries/0_stateless`](https://github.com/ClickHouse/ClickHouse/tree/master/tests/queries/0_stateless))
through clickhouse-go.

The upstream test runner
([`tests/clickhouse-test`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/clickhouse-test))
runs each `.sql` test through `clickhouse-client` and compares the output with
the `.reference` file of the test. This harness is a small Go program that
imitates `clickhouse-client` well enough for the runner to drive it without
changes: the same flags, the same stdin and `--query` behavior, and the
`extract-from-config` shortcut.

The program sends every query through clickhouse-go, over the native or the
HTTP protocol. clickhouse-go always reads a result as `Native` blocks. The
program scans each row into the Go types that clickhouse-go selects
(`ColumnType.ScanType()`), and writes the values as `TabSeparated` text. Thus
a test passes only when clickhouse-go decodes and scans every value correctly.

It is a port of the
[Java](https://github.com/ClickHouse/clickhouse-java/tree/main/tests/clickhouse-client)
and [JavaScript](https://github.com/ClickHouse/clickhouse-js/tree/main/tests/clickhouse-test-runner)
harnesses.

## Run the tests locally

You need Go, Python 3 with `jinja2`, and Docker.

1. Start the pinned ClickHouse server. The version is in
   [`clickhouse-version`](clickhouse-version):

   ```bash
   CLICKHOUSE_VERSION=$(tests/clickhouse-test-runner/scripts/pinned-version.sh) make up
   ```

2. Get the upstream tests of the same version:

   ```bash
   tests/clickhouse-test-runner/scripts/fetch-upstream.sh
   ```

   The script checks out the release tag of the pinned version (for example
   `v26.10.4.207-stable`) into `.upstream/ClickHouse`, sparse and shallow.
   The `.reference` files of that tag were made by the same server build. The
   script also generates `errorcodes.go` from the same checkout. To try
   another version, give it as the argument (`fetch-upstream.sh 26.11.1.10`),
   and start the server of that version.

   The run script stops when the server and the tests are of different
   versions, because then the `.reference` files do not match. Set
   `ALLOW_VERSION_MISMATCH=1` to run anyway.

3. Run the allowlisted tests:

   ```bash
   pip install jinja2
   tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   CLICKHOUSE_TEST_RUNNER_PROTOCOL=http tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8
   ```

The script builds the program into `.bin/clickhouse` (with a
`clickhouse-client` symlink), puts `.bin` first in `PATH` for the runner only,
and starts `tests/clickhouse-test`. It gives the runner
`--no-stateful --no-random-settings --no-random-merge-tree-settings`; other
arguments go to the runner after these. For example, to run one test that is
not in the allowlist:

```bash
UPSTREAM_TEST_LIST=<(echo 00001_select_1) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

The runner writes the output of a failed test next to the test, in
`.upstream/ClickHouse/tests/queries/0_stateless/<test>.stdout`, `.stderr` and
`.diff`.

The unit tests of the harness need no server. The live tests compare the
output of the program with the server's own `TabSeparated` output, and need
the server of step 1:

```bash
CLICKHOUSE_TEST_RUNNER_LIVE=1 go test ./tests/clickhouse-test-runner/
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `CLICKHOUSE_TEST_RUNNER_PROTOCOL` | `native` | The clickhouse-go protocol: `native` or `http`. |
| `UPSTREAM_CLICKHOUSE_DIR` | `.upstream/ClickHouse` | The checkout of `ClickHouse/ClickHouse`. |
| `UPSTREAM_TEST_LIST` | `allowlist.txt` | The file with the tests to run. |
| `SHARD_INDEX`, `SHARD_TOTAL` | `1`, `1` | Run only shard `SHARD_INDEX` of `SHARD_TOTAL` round-robin shards of the list. |
| `CLICKHOUSE_CLIENT_CLI_LOG` | (none) | A file. Each invocation of the program adds its arguments, statements and errors to it. |
| `CLICKHOUSE_PORT_TCP`, `CLICKHOUSE_PORT_HTTP` | `9000`, `8123` | The ports of the server. The runner gives `--port=$CLICKHOUSE_PORT_TCP`; with `http`, the program uses `CLICKHOUSE_PORT_HTTP` for it. |

## The allowlist

[`allowlist.txt`](allowlist.txt) lists the tests that pass reliably, one test
name (the file name without `.sql`) per line. `#` starts a comment. The script
gives the runner one anchored regular expression, so each name selects
exactly one test. (The runner itself matches each name as a substring, so
`00396_uuid` would also select `00396_uuid_v7`.)

The file has three kinds of lines:

| Line | Meaning |
|---|---|
| `00001_select_1` | The test runs over both protocols. |
| `00015_totals_having_constants  # only=native: <reason>` | The test runs over the native protocol only, because it cannot pass over HTTP for an environment cause (for example, the server sends no `WITH TOTALS` row in the HTTP `Native` output). |
| `# 01802_toDateTime64_large_values  # [dt64-range]` | A **known failure**: the test does not run, because of a bug in clickhouse-go or a gap in the shim. The ID in brackets names the group. The header of the group, higher in the file, describes the bug, gives a small reproduction and links the issue. |

To run the known failures of one or more groups, for example to check a fix:

```bash
KNOWN_FAILURES=dt64-range tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
KNOWN_FAILURES=all CLICKHOUSE_TEST_RUNNER_PROTOCOL=http tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
```

When a fix makes the tests of a group pass, remove the `# ` at the start and
the `# [<id>]` at the end of their lines. When all tests of a group pass,
remove the group header.

### Grow the allowlist

1. Run the candidate tests through the harness over **both** protocols. To
   try all `.sql` tests:

   ```bash
   ls tests/clickhouse-test-runner/.upstream/ClickHouse/tests/queries/0_stateless \
     | sed -nE 's/\.sql(\.j2)?$//p' | sort -u > /tmp/candidates.txt
   for p in native http; do
     CLICKHOUSE_TEST_RUNNER_PROTOCOL=$p UPSTREAM_TEST_LIST=/tmp/candidates.txt \
       tests/clickhouse-test-runner/scripts/run-upstream-tests.sh -j 8 --no-long --timeout 120 \
         --max-failures-chain 1000000 | tee /tmp/run-$p.log
   done
   ```

   Run the protocols one after the other, not at the same time: the memory
   pressure on the server causes `MEMORY_LIMIT_EXCEEDED` failures that are
   not real. `--max-failures-chain` turns off a limit of the runner: by
   default it stops after 20 failures in a row, which a list of failing
   tests reaches at once.

2. Keep only the tests that report `[ OK ]` over both protocols:

   ```bash
   comm -12 \
     <(sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run-native.log | sort -u) \
     <(sed -nE 's/^.*\] ([^ :]+): +\[ OK \].*/\1/p' /tmp/run-http.log | sort -u)
   ```

3. Run the new tests again (two or more times, and with `-j` higher than CI
   uses) and remove the tests that do not pass every time.

4. Make sure that the tests also pass on a fresh server, because CI starts
   from one. For example, a test that reads `system.query_log` can depend on
   the queries of other tests.

**Rule:** add only tests that pass reliably. Before you remove a test or
decide not to add it, find out whether it fails because of a bug in
clickhouse-go. The harness exists to find those bugs. See
[`AGENTS.md`](AGENTS.md) for the steps.

To compare with the real `clickhouse-client`, run the same tests through it,
in the server container:

```bash
UPSTREAM_TEST_LIST=<(echo 01802_toDateTime64_large_values) \
  tests/clickhouse-test-runner/scripts/run-with-real-client.sh
```

A test that passes there and fails through clickhouse-go shows a bug in
clickhouse-go or in this program.

## How the program imitates clickhouse-client

The program sends nothing that `clickhouse-client` does not send, because
some tests read their own queries from `system.query_log`.

- **Statements.** The program splits the input into statements as
  `clickhouse-client` does: at a top-level `;`, with the same-line trailing
  comment kept with its statement. It sends each statement with its `;`. The
  inline data of `INSERT ... FORMAT <name>` (other than `Values`) ends at an
  empty line. The `-- Tags:` header is not part of a statement. Every
  statement runs, with or without `--multiquery`.
- **Test hints.** `-- { serverError X }`, `-- { clientError X }` and
  `-- { error X }` (codes or names) make the error the expected result. A hint
  in a leading comment is ignored, as upstream. Over clickhouse-go the server
  finds all errors, so a server error can satisfy a `clientError` hint.
  `-- { echo }`, `-- { echoOn }` and `-- { echoOff }` print the statements,
  but not a statement with a syntax error (`clickhouse-client` parses before
  it echoes). `-- { retry ... }` is not supported.
- **Sessions.** `SET`, `USE` and temporary tables last for the whole
  invocation. Over HTTP, all requests have the same `session_id`, and a `USE`
  opens the connection again with the new database (clickhouse-go sends the
  database of the connection with each request). Over the native protocol,
  the session is the TCP connection. clickhouse-go closes the connection
  after any error, also after a server exception that a hint expects; the
  program then runs the `USE` and `SET` statements of the session again on
  the new connection. Other session state, such as a temporary table, is
  lost.
- **Statement types.** Statements that start with `SELECT`, `WITH`, `FROM`,
  `SHOW`, `DESC`, `DESCRIBE`, `EXISTS`, `EXPLAIN` or `CHECK` run with
  `Conn.Query`; all other statements run with `Conn.Exec`. After
  `SET dialect` to another language (KQL, PRQL, ...) or
  `SET implicit_select = 1`, all queries run with `Conn.Query`.
- **Output formats.** clickhouse-go always reads results as `Native`, and the
  program writes them itself in the `TabSeparated` family (also `TSVRaw` and
  `...WithNames[AndTypes]`) or `Null`. A statement with any other output
  format fails. Over native, the program sends the statement as it is (the
  server ignores the `FORMAT` clause). Over HTTP, it removes the clause,
  except for `FORMAT Null`.
- **Settings.** Of the settings on the command line, only `log_comment`,
  `max_insert_threads`, `max_threads` and `send_logs_level` go to the server,
  with each query, as in `clickhouse-client`. A `SET` of the same setting in
  the test replaces the command-line value. `--param_<name>=<value>` gives a
  query parameter.
- **Errors.** An unexpected error stops the program. It writes the error with
  its code and name to stderr, and the exit code is the error code modulo 256.
- **Error names.** [`errorcodes.go`](errorcodes.go) maps the error names of
  test hints to codes. [`scripts/fetch-upstream.sh`](scripts/fetch-upstream.sh)
  generates it from `src/Common/ErrorCodes.cpp` of the upstream checkout, so
  it always matches the tests. Commit the file when it changes.

## Known failures

The end of [`allowlist.txt`](allowlist.txt) lists the tests that fail
because of a bug in clickhouse-go or a gap in this program, in groups. Each
group header describes the cause and gives a small reproduction. A test that
cannot pass for an environment cause (a cluster, ZooKeeper, server
configuration, features of `clickhouse-client` itself) is not listed; see
[`AGENTS.md`](AGENTS.md#environment-causes). The program does not try `.sh`
tests.

## CI

[`upstream-sql-tests.yml`](../../.github/workflows/upstream-sql-tests.yml)
runs the allowlist nightly, manually, and on pushes and pull requests that
change this directory or the workflow.

- The matrix is `{protocol: native, http} × {shard: 1..N}`.
- The server version and the upstream tests are pinned together in
  [`clickhouse-version`](clickhouse-version). A new ClickHouse release
  changes nothing in CI, so a pull request never fails because of a release
  that it does not touch. A bump is a pull request of its own; see
  [`AGENTS.md`](AGENTS.md#bump-the-server-version).
- The `clickhouse_version` input of a manual run tries another version
  without a commit, for example before a bump.
- When an allowlisted test is not in the checkout (upstream renamed or
  removed it), the run shows a warning with the names.
- If a shard takes much more than a few minutes, increase the number of
  shards: change the `shard` list and `SHARD_TOTAL` together.
