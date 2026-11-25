# Instructions for agents: upstream SQL test harness

This harness runs the ClickHouse core SQL tests through clickhouse-go. See
[`README.md`](README.md) for how it works and how to run it.

## The goal

The goal is to find bugs in clickhouse-go. To pass more tests is not the
goal. A failed test is information. Do not hide it.

## Rules

1. **Find the cause of a failure before you do anything with the test.** A
   test fails for one of three causes:
   - **The client** (clickhouse-go, or `ch-go`, which it uses to decode the
     data). This is what the harness is for.
   - **The shim** (the program in this directory). It does not do what
     `clickhouse-client` does.
   - **The environment.** The test cannot pass through any client on this
     server (see [Environment causes](#environment-causes)).
2. **The default for a client bug is a fix in the client.** Open a pull
   request that fixes clickhouse-go, with a regression test in
   `tests/issues/` (see the root `CLAUDE.md`). Such a pull request does not
   start the workflow of the harness: to check that the fix breaks no test
   of the allowlist, add the `tests:run-core-sql` label (see the README,
   CI). When the bug is in `ch-go`, report it in `ClickHouse/ch-go` and link
   the report from the clickhouse-go issue.
3. **The default for a shim bug is a fix in the shim.** Make the shim copy
   `clickhouse-client` more exactly. Do not change the shim to hide a client
   bug, for example by a different scan type or a workaround for a wrong
   value. The shim must use the client as a user does: scan each value into
   `ColumnType.ScanType()`.
4. **Remove a test from the allowlist only when it cannot pass for an
   environment cause** (see rules 6 and 7). Write the reason in the commit
   message.
5. **A test that fails because of a client bug or a shim gap stays
   visible.** Do not delete it. Put it in [`allowlist.txt`](allowlist.txt)
   as a known failure, `# <test>  # [<id>]`, in the group of its cause. Each
   group has a header that describes the bug, gives a small reproduction and
   links the issue. Make a new group when no group fits. See the README for
   the format and for how to run one group (`KNOWN_FAILURES=<id>`).
6. **A test that cannot pass over one protocol** for an environment cause
   (for example `WITH TOTALS` over HTTP) gets `# only=<protocol>: <reason>`.
   Do not use this for a client bug: use a known failure.
7. **Do not list a test that cannot pass for an environment cause** over
   both protocols.

## How to find the cause

Do these steps in this order.

1. **Run the test over both protocols:**

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   UPSTREAM_TEST_LIST=<(echo <test>) CLICKHOUSE_TEST_RUNNER_PROTOCOL=http tests/clickhouse-test-runner/scripts/run-upstream-tests.sh
   ```

   A test that passes over one protocol and fails over the other points to
   the client, unless the cause is in the list below.
2. **Run the test through the real `clickhouse-client`** in the server
   container, on the same server:

   ```bash
   UPSTREAM_TEST_LIST=<(echo <test>) tests/clickhouse-test-runner/scripts/run-with-real-client.sh
   ```

   If it also fails here, the cause is the environment. If it passes here,
   the cause is the client or the shim.
3. **Find the statement that fails.** Set `CLICKHOUSE_CLIENT_CLI_LOG` to a
   file; the log shows each statement and the error. Read the `.diff`,
   `.stdout` and `.stderr` files next to the test.
4. **Compare the shim with the server on that one statement.** Run it
   through the shim (`go run ./tests/clickhouse-test-runner -q "<SQL>"`)
   and through `curl` with `FORMAT TSV`. Then make the statement smaller
   until it is as small as possible.
5. **Decide between the client and the shim with a small Go program** that
   uses only the public clickhouse-go API (`Query`, `Scan`, `ScanType()`). If
   the program also gives a wrong value or an error, the bug is in the
   client. If the program gives the correct value, the bug is in the shim.
   Try both protocols.

### Environment causes

A test cannot pass through any client on the CI server when it needs one of
these:

- A cluster, shards or replicas (`CLUSTER_DOESNT_EXIST`, `test_shard_localhost`).
- ZooKeeper or Keeper (`NO_ZOOKEEPER`, `Replicated*` engines).
- External services: S3, Azure, HDFS, Kafka, MySQL, PostgreSQL, MongoDB, URL
  sources.
- Server configuration of the upstream CI: named collections, dictionaries,
  storage policies, disks, users and grants, macros
  (`NAMED_COLLECTION_DOESNT_EXIST`, `NO_ELEMENTS_IN_CONFIG`, `UNKNOWN_POLICY`,
  `UNKNOWN_DISK`).
- Features that the server build or platform does not have
  (`SUPPORT_IS_DISABLED`, "not supported on Darwin").
- Stateful datasets (`--no-stateful`) or long tests (`--no-long`).
- Behavior of the HTTP interface itself. For example, the server sends no
  `WITH TOTALS` and no `extremes` in the `Native` format over HTTP, and an
  HTTP request fails when the current database of the session was dropped.
- Features of `clickhouse-client` itself that are not part of a driver:
  `INTO OUTFILE`, `FROM INFILE`, client-side parsing of `INSERT` data, and
  client-only settings.
- The tz database. ClickHouse and Go can give different historical offsets
  for the same zone (for example `Europe/Amsterdam` before 1937).
- Tests that conflict with other tests of the set: a fixed table UUID, or
  server state that a test leaves behind (failpoints that a failed test did
  not disable, row policies). Such a test passes alone on a fresh server and
  fails in a full run. A failure message with "Injected failure" comes from a
  failpoint of another test.
- Memory limits of the server, when the test also fails through the real
  `clickhouse-client`.

These are **not** environment causes. Find the bug:

- A value that is different from the server's value.
- A decode error, or an error such as `unexpected packet`.
- A `ScanType()` that `Scan` does not accept.
- A failure over one protocol only, when the cause is not in the list above.

## Conditions for the allowlist

- Add a test only when it passes over **both** protocols, or over one
  protocol with an `only=` reason (rule 6).
- Run new tests three times, also with a higher `-j` than CI, on a **fresh**
  server (`make down && CLICKHOUSE_VERSION=latest make up`). Tests change the
  state of the server (row policies, settings profiles, users). A test that
  passes only after other tests ran must not go in.
- Do not run two sweeps at the same time on one server. The memory pressure
  causes `MEMORY_LIMIT_EXCEEDED` failures that are not real.
- CI runs the failed tests again, at most twice (`RETRY_FAILED=2`), because
  of the pool race. Read the warnings of the passed-on-retry tests in the
  nightly runs: a test that is there often has a cause of its own, or hits
  the race often enough to belong in its group. Do not use the retry when you
  grow or check the allowlist.
- When a test in the allowlist starts to fail intermittently, find the cause
  first (rule 1). Intermittent native failures of tests that use `SET` can
  come from the pool race of `[native-pool-race-new-connection]`: the session
  loses the `SET`. Remove it only when the cause is in the environment or in
  the test itself, and write the reason.

## Bump the server version

The server version and the upstream tests are pinned together in
[`clickhouse-version`](clickhouse-version). A new ClickHouse release changes
nothing in CI until a pull request bumps the pin, so other pull requests do
not fail because of a release. Bump the pin in a pull request of its own,
and make that pull request pass before it merges:

1. Select the version: a full version with a server image tag and a
   `v<version>-stable` tag upstream, normally the version of the `latest`
   image.
2. Try it: start a manual run with the `clickhouse_version` input, or run
   the allowlist locally on that version (`fetch-upstream.sh <version>`, and
   a fresh server of that version).
3. For each allowlisted test that now fails, find the cause (rule 1). A
   release changes the tests and the server output, so many tests can fail
   at the same time with no change in the client. To see whether the client
   is the cause, run the same test through the real client
   (`run-with-real-client.sh`) on the same server.
4. Run the known failures (`KNOWN_FAILURES=all`). Enable the tests that pass
   now.
5. Fix the names of allowlisted tests that the run reports as not in the
   checkout (upstream renamed or removed them).
6. Optionally, try the new tests of the release (see "Grow the allowlist" in
   the README).
7. Change `clickhouse-version`, run `fetch-upstream.sh`, and run
   `gen-formats.sh` against a server of the new version. Commit
   `errorcodes.go` and `formats.go` with the new version.

## Keep the documents in sync

When you change the behavior of the shim (flags, statement handling, output
formats, settings), update [`README.md`](README.md), this file and the
workflow in the same change.
