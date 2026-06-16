#!/usr/bin/env python3
"""Post a Claude PR review as inline comments plus one updating summary comment.

This script is the deterministic half of the review skill: Claude produces a
findings JSON (see SKILL.md for the schema), and this script turns it into a
GitHub Pull Request *review* with line-anchored inline comments, plus a single
summary comment that is updated in place on every re-run.

Why a script instead of letting the model call the API directly:
  - The Reviews API rejects the *entire* review if any inline comment points at
    a line that is not part of the diff. The model cannot reliably know which
    lines are commentable, so we validate every finding against the diff hunks
    here and demote off-diff findings into the summary instead of failing.
  - Re-running the review must not spam the PR. We embed a hidden marker in
    every comment body and skip findings whose marker already exists, and we
    PATCH the existing summary comment rather than creating a new one.

It shells out to `gh api`, which must be authenticated (the workflow provides
GH_TOKEN). Standard library only — no third-party dependencies.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys

SUMMARY_MARKER = "<!-- claude-review:summary -->"
INLINE_MARKER_RE = re.compile(r"<!-- claude-review:inline:([0-9a-f]{12}) -->")

VALID_SEVERITIES = ("must_fix", "should_fix", "nit")
SEVERITY_LABELS = {
    "must_fix": "❌ Must fix",
    "should_fix": "⚠️ Should fix",
    "nit": "💡 Nit",
}
VERDICT_LABELS = {
    "approve": "✅ Approve",
    "request_changes": "⚠️ Request changes",
    "needs_discussion": "💬 Needs discussion",
}


def gh(args, payload=None):
    """Run a `gh` command, optionally piping a JSON payload to stdin.

    Returns parsed JSON when the command emits any, else None. Raises
    CalledProcessError on failure so the caller can decide whether to abort.
    """
    proc = subprocess.run(
        ["gh", *args],
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {proc.stderr.strip()}")
    out = proc.stdout.strip()
    if not out:
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return out


def diff_commentable_lines(repo, pr):
    """Return {path: set(new_file_line_numbers)} for lines commentable on RIGHT.

    Parses the unified diff and tracks new-file line numbers for added (`+`) and
    context (` `) lines. Removed lines (`-`) do not advance the new-file counter
    and are not commentable on the RIGHT side.
    """
    proc = subprocess.run(
        ["gh", "pr", "diff", str(pr), "--repo", repo],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"gh pr diff failed: {proc.stderr.strip()}")

    commentable = {}
    path = None
    new_line = 0
    hunk_re = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")
    for raw in proc.stdout.splitlines():
        if raw.startswith("+++ b/"):
            path = raw[6:]
            commentable.setdefault(path, set())
            continue
        if raw.startswith("+++ ") or raw.startswith("--- "):
            continue
        m = hunk_re.match(raw)
        if m:
            new_line = int(m.group(1))
            continue
        if path is None:
            continue
        if raw.startswith("+"):
            commentable[path].add(new_line)
            new_line += 1
        elif raw.startswith("-"):
            pass
        elif raw.startswith("\\"):  # "\ No newline at end of file"
            pass
        else:  # context line
            commentable[path].add(new_line)
            new_line += 1
    return commentable


def inline_key(path, line, body):
    """Stable per-finding key, robust to trivial whitespace edits in the body."""
    normalized = " ".join(body.split())
    digest = hashlib.sha1(f"{path}:{line}:{normalized}".encode()).hexdigest()
    return digest[:12]


def existing_inline_keys(repo, pr):
    comments = gh(
        ["api", f"repos/{repo}/pulls/{pr}/comments", "--paginate"]
    ) or []
    keys = set()
    for c in comments:
        for m in INLINE_MARKER_RE.finditer(c.get("body", "")):
            keys.add(m.group(1))
    return keys


def find_summary_comment(repo, pr):
    comments = gh(
        ["api", f"repos/{repo}/issues/{pr}/comments", "--paginate"]
    ) or []
    for c in comments:
        if SUMMARY_MARKER in c.get("body", ""):
            return c["id"]
    return None


def render_finding_body(finding):
    label = SEVERITY_LABELS.get(finding["severity"], finding["severity"])
    title = finding.get("title", "").strip()
    body = finding.get("body", "").strip()
    header = f"**{label}**"
    if title:
        header += f" — {title}"
    return f"{header}\n\n{body}"


def render_summary(findings_json, general, demoted):
    verdict = findings_json.get("verdict", "needs_discussion")
    lines = [
        SUMMARY_MARKER,
        "## 🤖 Claude review",
        "",
        findings_json.get("summary", "").strip(),
        "",
        f"**Verdict:** {VERDICT_LABELS.get(verdict, verdict)}",
    ]

    general_and_demoted = list(general) + list(demoted)
    if general_and_demoted:
        lines += ["", "### General findings", ""]
        for f in general_and_demoted:
            label = SEVERITY_LABELS.get(f["severity"], f["severity"])
            loc = f.get("_loc", "")
            loc = f" (`{loc}`)" if loc else ""
            title = f.get("title", "").strip()
            head = f"- **{label}**{loc}"
            if title:
                head += f" — {title}"
            lines.append(head)
            body = f.get("body", "").strip()
            if body:
                for bl in body.splitlines():
                    lines.append(f"  {bl}")

    lines += [
        "",
        "<sub>Inline comments are attached to the relevant lines. "
        "This summary updates in place on re-review.</sub>",
    ]
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--pr", required=True)
    ap.add_argument("--input", required=True, help="path to findings JSON")
    args = ap.parse_args()

    with open(args.input) as fh:
        data = json.load(fh)

    findings = data.get("findings", []) or []
    general = data.get("general_findings", []) or []

    commentable = diff_commentable_lines(args.repo, args.pr)
    already = existing_inline_keys(args.repo, args.pr)

    new_comments = []
    demoted = []
    skipped_dup = 0
    for f in findings:
        sev = f.get("severity")
        if sev not in VALID_SEVERITIES:
            f["severity"] = sev = "should_fix"
        path = f.get("path")
        line = f.get("line")
        body = f.get("body", "")
        if not path or not isinstance(line, int) or line not in commentable.get(path, set()):
            # Not anchorable to a diff line — surface it in the summary instead
            # of dropping it or failing the whole review.
            f["_loc"] = f"{path}:{line}" if path and line else (path or "")
            demoted.append(f)
            continue
        key = inline_key(path, line, body)
        if key in already:
            skipped_dup += 1
            continue
        marked_body = f"{render_finding_body(f)}\n\n<!-- claude-review:inline:{key} -->"
        new_comments.append(
            {"path": path, "line": line, "side": "RIGHT", "body": marked_body}
        )

    # Post a review only if there are genuinely new inline comments. An empty
    # review with no body is rejected by the API, and re-posting an empty one
    # would be noise.
    if new_comments:
        gh(
            [
                "api",
                f"repos/{args.repo}/pulls/{args.pr}/reviews",
                "--method",
                "POST",
                "--input",
                "-",
            ],
            payload={"event": "COMMENT", "comments": new_comments},
        )
        print(f"Posted {len(new_comments)} new inline comment(s).")
    else:
        print("No new inline comments to post.")
    if skipped_dup:
        print(f"Skipped {skipped_dup} inline finding(s) already present.")
    if demoted:
        print(f"Demoted {len(demoted)} off-diff finding(s) into the summary.")

    summary_body = render_summary(data, general, demoted)
    summary_id = find_summary_comment(args.repo, args.pr)
    if summary_id is not None:
        gh(
            [
                "api",
                f"repos/{args.repo}/issues/comments/{summary_id}",
                "--method",
                "PATCH",
                "--input",
                "-",
            ],
            payload={"body": summary_body},
        )
        print(f"Updated summary comment {summary_id}.")
    else:
        gh(
            [
                "api",
                f"repos/{args.repo}/issues/{args.pr}/comments",
                "--method",
                "POST",
                "--input",
                "-",
            ],
            payload={"body": summary_body},
        )
        print("Created summary comment.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # surface a clear CI failure
        print(f"post_review.py: {exc}", file=sys.stderr)
        sys.exit(1)
