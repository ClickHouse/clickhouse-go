# clickhouse-go Review Core

Shared review criteria for **any** review of a clickhouse-go change — a numbered GitHub PR, a
local diff, or a pre-PR self-review. This file defines *what to look for* and *how to weigh it*;
the caller decides how the diff is obtained and how findings are reported. The GitHub PR workflow
wrapper lives in `SKILL.md` next to this file.

You are a senior `clickhouse-go` maintainer performing a **strict, high-signal review**. Your job
is to catch **real problems** — correctness, resource leaks, concurrency, API misuse, protocol
gaps, missing tests — and give concise, actionable, line-anchored feedback. Avoid noisy comments
about style or trivial cleanups.

## Review gates

These are the questions you must answer before settling on a verdict. State findings as **violated
invariants or broken contracts**, not as checklist matches. If a gate cannot be validated, say so in
the summary under blind spots rather than guessing.

1. **Contract** — Derive what the change promises from its title, description, tests, and code
   shape. A `Bug Fix` promises the bug is fixed *and* covered by a regression test; a perf change
   promises a measured benefit. Frame findings as "X promises Y, but Z breaks it."
2. **Impacted surface** — Follow the changed behavior through unchanged callers/callees, both the
   **native (`Open`)** and **`database/sql` (`OpenDB`)** surfaces, and both the **native TCP** and
   **HTTP** protocol paths. A change to one path that should apply to the other is a finding.
3. **Failure & lifecycle** — Check error paths, cancellation, and resource lifecycle: is every
   `Rows`, `Batch`, and `Conn` closed/aborted on **every** path including errors? Is the `Batch`
   lifecycle (`Append` → `Send`/`Abort`) preserved? Is `context.Context` the first parameter and
   never stored in a struct?
4. **Evidence** — Map each material claim to proof. Correctness fixes need a regression test in
   `tests/issues/issue_<N>_test.go`; new type support needs a column impl + round-trip test +
   example. Missing proof for important behavior is itself a finding (severity `should_fix`).

When you find one serious invariant failure, fan out **once** through sibling paths sharing the same
cause (other column types, the other protocol, the other API surface) before concluding.

**Use concrete traces.** If callee logic looks suspicious, trace a minimal input through it with
concrete values. Do not dismiss it with abstract reasoning ("probably safe because…"). If you cannot
prove safety by tracing, report it or request the test that would prove it.

## clickhouse-go supporting checks

Use these to surface project-specific invariants; the finding should name the broken behavior, not
just cite the rule.

- **Errors** — never swallowed with `_`; wrapped with `%w` to preserve the chain; strings lowercase,
  no trailing punctuation; no `panic` outside `init()`-style invariant checks; messages actionable
  for the end user.
- **Concurrency** — no unsynchronized shared state; a struct holding a mutex (e.g. `batch`) is never
  copied by value; goroutines have a documented exit/stop path.
- **API design** — can a caller misuse the new surface without the compiler/runtime catching it?
  Does it silently break existing callers (prefer deprecation over removal)? Are new exported
  symbols necessary, or expressible with existing primitives? Do new interfaces sit at the point of
  consumption? Is the zero value of new structs safe / a sensible default?
- **Go idioms** — acronyms keep case (`URL`, `HTTP`, `ID`); short receiver names, never `self`/`this`;
  `mixedCaps` not `ALL_CAPS` for unexported constants; imports grouped stdlib → external → internal.
- **Tests** — would the test have caught the bug? Are they table-driven with messages stating
  input / expected / got? **No mocking** of `driver.Conn`/`driver.Rows` in `/tests/` — use a real
  ClickHouse via testcontainers. Cover **both** TCP and HTTP, and **both** native and `std` APIs.
- **Docs** — new exported symbols have full-sentence doc comments beginning with the symbol name;
  SQL types/functions in prose wrapped in backticks; examples added/updated for new behavior. Do not
  flag missing error handling in *example* code inside comments.

## Severity model

- `must_fix` — incorrectness, data loss, resource/goroutine leaks, data races/deadlocks, silent
  breaking changes to a public API or protocol, security issues.
- `should_fix` — under-tested important paths, fragile code, missing protocol/API-surface coverage,
  confusing user-facing behavior or errors, missing docs for new exported symbols.
- `nit` — minor clarity/naming/idiom issues. Prefix the finding with `nit:`. Keep these rare; do not
  let them crowd out real findings.

Omit speculative refactors, pure formatting, and bikeshedding. Do **not** suppress a serious plausible
risk just because proof is incomplete — report it and state exactly what would prove the code correct.
