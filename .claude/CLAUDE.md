# clickhouse-go — Agent Instructions

`github.com/ClickHouse/clickhouse-go/v2` is the official Go client for ClickHouse. It exposes two surfaces:
- **Native API** (`clickhouse.Open`) — full-featured, direct `driver.Conn` interface.
- **Standard library** (`database/sql`) — compatibility shim via `clickhouse_std.go`.

For build, test, and local setup instructions see `CONTRIBUTING.md` and `README.md`.

---

## Interfaces at a Glance

All user-facing types are in `lib/driver/driver.go`.

| Interface    | Obtained from               | Key responsibility |
|--------------|-----------------------------|--------------------|
| `Conn`       | `clickhouse.Open()`         | Query, batch, exec, ping |
| `Rows`       | `Conn.Query()`              | Iterate result set |
| `Row`        | `Conn.QueryRow()`           | Single-row result  |
| `Batch`      | `Conn.PrepareBatch()`       | Bulk insert lifecycle |

`Conn` is the only entry point. Do not add package-level globals or alternative constructors without a clear reason.

---

## Go Idioms Enforced Here

Follow [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments) in full. The rules below are the ones that matter most in this codebase.

**Errors**
- Never discard errors with `_`. Return, handle, or wrap — never swallow.
- Error strings: lowercase, no trailing punctuation — `"clickhouse: connection closed"`.
- Wrap with `fmt.Errorf("context: %w", err)` to preserve the chain; sentinel errors live in `clickhouse.go`.
- Do not `panic` in library code. Panics belong only in `init()` for invariants that can never recover.
- Error message returned to the end-user should be actionable as possible.

**Context**
- Every network call takes `context.Context` as its first parameter — no exceptions.
- Never store a `Context` in a struct field.
- Deadlines and timeouts are the caller's responsibility, not the library's.

**Interfaces**
- Define interfaces at the point of *consumption*, not *implementation* (`lib/driver` is consumed by both the root package and by users).
- Do not introduce an interface solely to enable test mocking. Write integration tests against a real ClickHouse instance.
- Prefer small, focused interfaces. If a new interface needs more than ~5 methods, reconsider the design.

**Naming**
- Receiver names: short abbreviation of the type (`c` for `conn`, `b` for `batch`). Never `self` or `this`.
- Acronyms keep their case: `URL`, `HTTP`, `DSN`, `ID` — not `Url`, `Http`, `Dsn`, `Id`.
- Unexported constants: `mixedCaps`, never `ALL_CAPS`.
- Avoid meaningless package names: `util`, `common`, `helpers`, `misc`.

**Structs & ownership**
- `batch` holds a mutex — never copy it by value; always use a pointer.
- Column implementations in `lib/column/` use pointer receivers throughout; stay consistent.

**Goroutines**
- Always document when a goroutine exits and how it is stopped.
- Prefer synchronous APIs. Async variants (e.g., `WithAsync`) are opt-in through context, not the default path.

**Imports**
- Three groups separated by blank lines: stdlib → external → internal (`github.com/ClickHouse/clickhouse-go`).
- Do not rename imports except to resolve collisions.

---

## Design Principle

**Make the API easy to use correctly and hard to use incorrectly.**

Every API decision in this driver reflects this principle:

- `Batch.Send()` marks the batch as sent; all subsequent calls return `ErrBatchAlreadySent`. You cannot accidentally double-send.
- `Rows` must be explicitly closed. Leaving it open holds a connection. `Stats()` exposes pool pressure so leaks are visible.
- Parameter binding (`args ...any`) prevents SQL injection. Never interpolate user input into query strings.
- `context.Context` is required on every call — callers cannot accidentally fire a query without a cancellation path.
- `OpError` carries the operation name and column name — callers get structured error information without string parsing.
- Deprecated APIs (`AsyncInsert`) remain available alongside their replacements — callers are never broken silently.

When proposing a new API surface, ask: *can a caller use this incorrectly without the compiler or runtime catching it?* If yes, redesign.

---

## Workflow

- Do not commit directly to `main`. All changes go through a pull request.
- Do not rebase or amend commits on a shared branch — add new commits instead.
- Do not force-push to `main`.
- Regression tests for bug fixes go in `tests/issues/` named after the issue number: `issue_1234_test.go`.
- New ClickHouse type support requires: a column implementation in `lib/column/`, a round-trip test in `tests/`, and an example in `examples/clickhouse_api/`.
- When referencing ClickHouse SQL types, functions, or log messages in text, wrap them in backticks: `` `DateTime64` ``, `` `MergeTree` ``.
