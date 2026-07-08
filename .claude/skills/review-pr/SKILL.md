---
name: review-pr
description: Review a numbered GitHub pull request for correctness, API safety, Go idioms, and protocol coverage, then post inline comments plus one updating summary. Use only when the user asks to review an open PR by number. For a local or pre-PR diff, do not use this skill — apply review-core.md in this directory instead.
argument-hint: "<PR-number>"
allowed-tools: Read, Glob, Grep, Bash(grep:*), Bash(gh pr view:*), Bash(gh pr diff:*), Bash(gh api:*), Bash(python3:*), Write
---

# clickhouse-go PR Review Skill

Reviews an open GitHub pull request and posts the findings. The review criteria — the review
gates, the clickhouse-go supporting checks, and the severity model — live in
[`review-core.md`](review-core.md) in this directory. **Read `review-core.md` first and apply
it**; this file adds only the GitHub plumbing: fetching the PR, the findings JSON schema, and
posting.

Reviewing a local diff with no PR (e.g. before opening one)? Use `review-core.md` directly and
report findings as plain text — none of the JSON/posting machinery below applies.

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

### Existing review threads (re-review)

If `existing-threads.json` is present (the CI workflow writes it; create it for interactive runs
with `python3 .claude/skills/review-pr/post_review.py fetch --repo ClickHouse/clickhouse-go --pr "$0" > existing-threads.json`),
`Read` it. It lists the review threads you already started, each with its prior comment(s), any
author replies, and `is_resolved`/`is_outdated` state. For **every** open (`is_resolved: false`)
thread, decide an action (section 3, `thread_actions`):

- The author replied with a question or pushback that still stands → **reply** addressing it.
- The concern is now addressed (the current code satisfies it, or the author's reply adequately
  resolves/declines it) → **resolve** (optionally with a short closing reply).
- The concern still stands and there is no new author activity → **keep** (no new comment).
- The thread is `is_outdated` and the underlying concern no longer applies → **resolve**.

Do **not** raise a brand-new `findings` entry for a line that already has one of your threads — use
a `thread_actions` reply instead, or the poster will skip it as a duplicate.

## 2. Review the change

Work through the review gates and the clickhouse-go supporting checks from
[`review-core.md`](review-core.md), and grade each finding with its severity model. Anchor each
finding to a specific changed line where possible.

## 3. Output: write the findings JSON

Anchor every finding you can to a specific changed line so it becomes an **inline** comment. Write
the result to `claude-review.json` (in the repo root) with this exact schema:

```json
{
  "summary": "Structured markdown (see 'Writing the summary' below): a short intro line, then short paragraphs and/or bullet lists. Use `\\n` for line breaks. Cover what the PR does, the high-level verdict, and any blind spots.",
  "verdict": "approve | request_changes | needs_discussion",
  "findings": [
    {
      "path": "lib/column/date.go",
      "line": 142,
      "severity": "must_fix",
      "title": "short imperative title",
      "body": "Structured markdown (see 'Writing inline comments' below): lead with one sentence naming the broken invariant and its impact, then bullets when there is more than one point, then a ```suggestion``` block or diff for the fix. Use `\\n` for line breaks. Keep it short."
    }
  ],
  "general_findings": [
    {
      "severity": "should_fix",
      "title": "missing HTTP-path regression test",
      "body": "Findings that are NOT anchorable to a changed line (cross-cutting, or about code outside the diff). These render in the summary comment."
    }
  ],
  "thread_actions": [
    {
      "thread_id": "PRRT_kwDO...",
      "root_comment_id": 3421542714,
      "action": "reply | resolve | keep",
      "reply_body": "Required for action=reply. Optional closing note for action=resolve. Omit for keep."
    }
  ]
}
```

`thread_actions` is only for re-reviews where `existing-threads.json` listed open threads; copy
`thread_id` and `root_comment_id` verbatim from that file. Omit the array (or leave it empty) on a
first review. The poster skips replies it has already posted, so re-running is safe.

### Writing the summary

The `summary` string is rendered verbatim as markdown in the PR comment, so format it for fast
scanning — **never a single dense paragraph**. Keep it tight; the inline comments carry the detail.

- Lead with **one or two sentences** stating what the PR does and the overall verdict.
- Break the rest into **short paragraphs** (2–3 sentences each) separated by a blank line (`\n\n`),
  one idea per paragraph.
- Use a **bullet list** (`\n` between `- ` items) whenever you are enumerating things — affected
  surfaces, blind spots, follow-ups, or the key issues driving the verdict. Bullets beat prose for
  any list of two or more items.
- Bold short inline labels (e.g. `**Blind spots:**`) to anchor sections when it aids skimming.
- Do not restate every inline finding here; reference them collectively and highlight only what
  shapes the verdict.

Example shape (adapt freely):

```
Adds `DateTime64` scale handling to the native path. The core change is sound, but the HTTP
path is left uncovered.

**Key concerns:**
- Scale > 9 silently overflows (see inline on `lib/column/datetime64.go`).
- No regression test for the `std` API surface.

**Blind spots:** could not validate the HTTP round-trip without a live server.
```

### Writing inline comments

Each finding's `body` renders as markdown directly beneath a bold severity + title header that
the poster prepends (`**❌ Must fix** — <title>`), so do **not** repeat the title or severity in the
body. Apply the same scan-first discipline as the summary: a reviewer reading the comment on the line
should grasp the problem and the fix in one pass. **Never a single dense block of prose.**

- Lead with **one sentence** naming the broken invariant and its concrete impact — e.g. "Scale > 9
  overflows the `int32` multiplier, so sub-second values silently truncate." No preamble ("I noticed
  that…", "It looks like…"); state the problem directly.
- Keep the whole comment **tight** (aim for under ~6 lines). The reviewer needs the bug and the fix,
  not exhaustive reasoning — push deep rationale to the summary or omit it.
- When the body has **more than one distinct point** (root cause, an affected sibling path, a test
  gap), use a **short bullet list** (`\n` between `- ` items) instead of stringing them into one
  paragraph. One point → one sentence is fine; don't pad it into bullets.
- Put any concrete fix in a ```suggestion``` block (GitHub applies it in one click) or a fenced diff,
  separated from the prose by a blank line (`\n\n`). Suggestion blocks must contain only the
  replacement line(s) for the commented range.

Example shape (indented here to show the literal markdown, including a nested suggestion block):

    `Send()` doesn't reset `b.sent` under the lock, so a concurrent `Append` races the flag and
    can slip a row into an already-sent batch.

    - The read in `Append` (line 88) is unsynchronized against this write.
    - The `std` wrapper hits the same path via `ExecContext`.

    ```suggestion
        b.mu.Lock()
        b.sent = true
        b.mu.Unlock()
    ```

Rules for the JSON:
- `line` is the line number in the **new** version of the file, and **must** be a line that appears
  in the diff (added or context). If a finding concerns code not in the diff, put it in
  `general_findings` instead — the poster will reject off-diff inline lines and demote them anyway,
  but placing them correctly keeps the output clean.
- One finding per distinct issue. Group issues that share a single root cause into one finding.
- If there are no findings, emit empty arrays and an `approve` verdict with a one-line summary.
- Do not invent line numbers. When unsure of the exact line, use `general_findings`.

## 4. Post the review

The CI workflow posts the JSON automatically as a separate step. When running **interactively**, post
it yourself:

```bash
python3 .claude/skills/review-pr/post_review.py post --repo ClickHouse/clickhouse-go --pr "$0" --input claude-review.json
```

`post_review.py post` is idempotent: it attaches inline comments to the relevant lines, skips
findings it already posted (so re-reviews do not duplicate), demotes any off-diff finding into the
summary, replies into existing threads, resolves addressed/outdated threads, and updates a single
summary comment in place instead of stacking new ones.
