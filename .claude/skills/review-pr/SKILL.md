---
name: review-pr
description: Review a pull request and post structured feedback as a comment.
argument-hint: "<PR-number>"
allowed-tools: Read, Glob, Bash(grep:*), Bash(gh pr view:*), Bash(gh pr diff:*), Bash(gh pr comment:*)
---

Review the pull request and provide structured feedback.

## How to fetch the PR

If given a PR number or URL, use `gh pr diff <number>` to get the diff and `gh pr view <number>` to get the description. Read any referenced issues for context.

## Review structure

Return feedback in this order:

1. **Summary** — one paragraph: what the PR does, why it exists, whether the approach is sound.
2. **Must fix** — blocking issues; the PR should not merge until these are resolved.
3. **Should fix** — non-blocking but important (correctness risks, API design concerns, missing tests).
4. **Nits** — style, naming, minor clarity improvements. Prefix each with `nit:`.
5. **Verdict** — one of: `Approve`, `Request changes`, or `Needs discussion`.

If a section has no items, omit it.

---

## Checklist

Work through each category before writing the review.

### Correctness
- [ ] Does the logic handle error cases? Are errors returned (not swallowed with `_`)?
- [ ] Are all `Rows`, `Batch`, and `Conn` values closed on every code path, including error paths?
- [ ] Are there data races? Does any shared state lack synchronisation?
- [ ] Does the `Batch` lifecycle (`Append` → `Send` or `Abort`) hold on every path?
- [ ] Is `context.Context` propagated correctly (first param, never stored in a struct)?

### API design — easy to use correctly, hard to use incorrectly
- [ ] Can a caller misuse the new API without the compiler or runtime catching it?
- [ ] Does the change silently break any existing callers? Prefer deprecation over removal.
- [ ] Are new exported types/functions/methods necessary? Could this be done with existing primitives?
- [ ] Do new interfaces belong at the point of consumption, not implementation?
- [ ] Is the zero value of any new struct useful or at least safe?

### Go idioms
- [ ] Error strings: lowercase, no trailing punctuation.
- [ ] Acronyms in names: `URL`, `HTTP`, `ID` — not `Url`, `Http`, `Id`.
- [ ] Receiver names: short abbreviation, never `self` or `this`.
- [ ] No `context.Context` stored in struct fields.
- [ ] No `panic` outside of `init()`-style invariant checks.
- [ ] Imports: stdlib → external → internal, blank line between groups.
- [ ] New types that contain a mutex are not copied by value.
- [ ] `mixedCaps` for unexported constants, not `ALL_CAPS`.

### Tests
- [ ] Is there a test that would have caught the bug being fixed?
- [ ] Bug fixes: is there a regression test in `tests/issues/issue_<N>_test.go`?
- [ ] New ClickHouse type support: column implementation + round-trip test + example?
- [ ] Do test failure messages say what was wrong, what input triggered it, and what was expected vs. got?
- [ ] Are tests table-driven where there are multiple cases?
- [ ] No test mocking of `driver.Conn` or `driver.Rows` for integrations tests inside root `/tests/` — use a real ClickHouse via testcontainers.
- [ ] Make sure tests are covered for both TCP and HTTP protocol cases.
- [ ] Make sure tests are covered for both `clickhouse_native` (Open() api returns) api and `std` api (OpenDB() api returns).

### Performance & protocol
- [ ] Does the change avoid unnecessary allocations in hot paths (encoding/decoding columns)?
- [ ] Are new `Options` fields safe to ignore when unset (i.e., the zero value is the sensible default)?
- [ ] Does the change affect both the native TCP and HTTP protocol paths? If so, are both covered?

### Documentation
- [ ] Are new exported symbols documented with a full-sentence doc comment beginning with the symbol name?
- [ ] ClickHouse SQL types and function names wrapped in backticks in any prose.
- [ ] Make sure existing docs and examples are updated.
- [ ] Make sure new docs and examples are added if needed for new features or bug fixes.
- [ ] Make sure the docs are covered for both TCP and HTTP protocol cases.
- [ ] Make sure the docs are covered for both `clickhouse_native` (Open() api returns) api and `std` api (OpenDB() api returns).
