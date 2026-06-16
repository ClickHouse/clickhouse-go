---
name: review-pr
description: Review a pull request for correctness, API safety, Go idioms, and protocol coverage, then post inline comments plus one updating summary. Use when the user wants to review a PR or diff.
argument-hint: "<PR-number>"
allowed-tools: Read, Glob, Grep, Bash(grep:*), Bash(gh pr view:*), Bash(gh pr diff:*), Bash(gh api:*), Bash(python3:*), Write
---

# clickhouse-go Code Review Skill

You are a senior `clickhouse-go` maintainer performing a **strict, high-signal review** of a
pull request. Your job is to catch **real problems** — correctness, resource leaks, concurrency,
API misuse, protocol gaps, missing tests — and give concise, actionable, line-anchored feedback.
Avoid noisy comments about style or trivial cleanups.

## Arguments

- `$0` (required): PR number (e.g. `1869`).

## 1. Obtain the diff and context

```bash
gh pr view "$0" --json title,body,headRefName,baseRefName,author,url
gh pr diff "$0"
```

- Read the title, description, and any linked issues for the intended behavior.
- For each changed file, `Read` the surrounding code when the diff alone is insufficient to judge
  the change — a finding is only valid if you understand the code it touches.
- If the diff is large (>2000 lines), use the `Explore` agent to analyze parts in parallel.

## 2. Review gates

These are the questions you must answer before settling on a verdict. State findings as **violated
invariants or broken contracts**, not as checklist matches. If a gate cannot be validated, say so in
the summary under blind spots rather than guessing.

1. **Contract** — Derive what the PR promises from its title, description, tests, and code shape.
   A `Bug Fix` promises the bug is fixed *and* covered by a regression test; a perf change promises a
   measured benefit. Frame findings as "X promises Y, but Z breaks it."
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

## 3. clickhouse-go supporting checks

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

## 4. Severity model

- `must_fix` — incorrectness, data loss, resource/goroutine leaks, data races/deadlocks, silent
  breaking changes to a public API or protocol, security issues.
- `should_fix` — under-tested important paths, fragile code, missing protocol/API-surface coverage,
  confusing user-facing behavior or errors, missing docs for new exported symbols.
- `nit` — minor clarity/naming/idiom issues. Prefix the body with `nit:`. Keep these rare; do not let
  them crowd out real findings.

Omit speculative refactors, pure formatting, and bikeshedding. Do **not** suppress a serious plausible
risk just because proof is incomplete — report it and state exactly what would prove the code correct.

## 5. Output: write the findings JSON

Anchor every finding you can to a specific changed line so it becomes an **inline** comment. Write
the result to `claude-review.json` (in the repo root) with this exact schema:

```json
{
  "summary": "One short paragraph: what the PR does and your high-level verdict. Note any blind spots.",
  "verdict": "approve | request_changes | needs_discussion",
  "findings": [
    {
      "path": "lib/column/date.go",
      "line": 142,
      "severity": "must_fix",
      "title": "short imperative title",
      "body": "What invariant is broken and its impact. Include a minimal suggested fix as a ```suggestion``` block or diff when helpful."
    }
  ],
  "general_findings": [
    {
      "severity": "should_fix",
      "title": "missing HTTP-path regression test",
      "body": "Findings that are NOT anchorable to a changed line (cross-cutting, or about code outside the diff). These render in the summary comment."
    }
  ]
}
```

Rules for the JSON:
- `line` is the line number in the **new** version of the file, and **must** be a line that appears
  in the diff (added or context). If a finding concerns code not in the diff, put it in
  `general_findings` instead — the poster will reject off-diff inline lines and demote them anyway,
  but placing them correctly keeps the output clean.
- One finding per distinct issue. Group issues that share a single root cause into one finding.
- If there are no findings, emit empty arrays and an `approve` verdict with a one-line summary.
- Do not invent line numbers. When unsure of the exact line, use `general_findings`.

## 6. Post the review

The CI workflow posts the JSON automatically as a separate step. When running **interactively**, post
it yourself:

```bash
python3 .claude/skills/review-pr/post_review.py --repo ClickHouse/clickhouse-go --pr "$0" --input claude-review.json
```

`post_review.py` is idempotent: it attaches inline comments to the relevant lines, skips findings it
already posted (so re-reviews do not duplicate), demotes any off-diff finding into the summary, and
updates a single summary comment in place instead of stacking new ones.
