#!/usr/bin/env python3
"""Deterministic posting half of the clickhouse-go review skill.

Claude produces a findings JSON (see SKILL.md for the schema); this script turns
it into a GitHub Pull Request *review* with line-anchored inline comments, a
single summary comment that is updated in place, replies into existing comment
threads, and resolution of threads whose concern has been addressed.

Two subcommands:

  fetch  Dump the review threads this bot already owns on the PR (its prior
         inline comments plus any human replies, with resolve/outdated state)
         as JSON. The review step reads this so it can decide, per open thread,
         whether to reply, resolve, or leave it alone.

  post   Apply a findings JSON: post new inline comments (deduped), update the
         summary comment in place, reply into threads, and resolve addressed or
         outdated threads.

Why a script instead of letting the model call the API directly:
  - The Reviews API rejects the *entire* review if any inline comment points at
    a line not in the diff, so we validate every finding against the diff hunks
    here and demote off-diff findings into the summary instead of failing.
  - Re-running must not spam the PR. Every comment carries a hidden marker; we
    skip findings/replies already posted, PATCH the existing summary rather than
    stacking a new one, and reply into threads instead of duplicating them.
  - Resolving a review thread is only possible via the GraphQL
    `resolveReviewThread` mutation; the REST API cannot do it.

Shells out to `gh` (must be authenticated; the workflow provides GH_TOKEN).
Standard library only — no third-party dependencies.
"""

import argparse
import hashlib
import json
import re
import subprocess
import sys

SUMMARY_MARKER = "<!-- claude-review:summary -->"
INLINE_MARKER_RE = re.compile(r"<!-- claude-review:inline:([0-9a-f]{12}) -->")
REPLY_MARKER_RE = re.compile(r"<!-- claude-review:reply:([0-9a-f]{12}) -->")
# A thread is "ours" if its root comment carries an inline marker.
OURS_RE = re.compile(r"<!-- claude-review:(inline|reply):[0-9a-f]{12} -->")

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

    Returns parsed JSON when the command emits any, else None. Raises on failure
    so the caller can decide whether to abort.
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
    """Return {path: set(new_file_line_numbers)} commentable on the RIGHT side.

    Tracks new-file line numbers for added (`+`) and context (` `) lines. Removed
    lines do not advance the new-file counter and are not commentable on RIGHT.
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
        elif raw.startswith("-") or raw.startswith("\\"):
            pass
        else:  # context line
            commentable[path].add(new_line)
            new_line += 1
    return commentable


def fetch_threads(repo, pr):
    """Return our review threads on the PR (those whose root comment is ours)."""
    owner, name = repo.split("/", 1)
    query = (
        "query($owner:String!,$name:String!,$number:Int!,$cursor:String){"
        "repository(owner:$owner,name:$name){pullRequest(number:$number){"
        "reviewThreads(first:100,after:$cursor){"
        "pageInfo{hasNextPage endCursor}"
        "nodes{id isResolved isOutdated path line "
        "comments(first:50){nodes{databaseId body author{login}}}}}}}}"
    )
    threads = []
    cursor = None
    while True:
        args = [
            "api", "graphql",
            "-f", f"query={query}",
            "-f", f"owner={owner}",
            "-f", f"name={name}",
            "-F", f"number={pr}",
        ]
        if cursor:
            args += ["-f", f"cursor={cursor}"]
        data = gh(args)
        rt = data["data"]["repository"]["pullRequest"]["reviewThreads"]
        for node in rt["nodes"]:
            comments = node["comments"]["nodes"]
            if not comments:
                continue
            root = comments[0]
            if not OURS_RE.search(root.get("body", "")):
                continue  # not our thread
            threads.append({
                "thread_id": node["id"],
                "root_comment_id": root["databaseId"],
                "path": node.get("path"),
                "line": node.get("line"),
                "is_resolved": node["isResolved"],
                "is_outdated": node["isOutdated"],
                "comments": [
                    {"author": c["author"]["login"] if c.get("author") else None,
                     "body": OURS_RE.sub("", c.get("body", "")).strip()}
                    for c in comments
                ],
            })
        if rt["pageInfo"]["hasNextPage"]:
            cursor = rt["pageInfo"]["endCursor"]
        else:
            break
    return {"threads": threads}


def inline_key(path, line, body):
    """Stable per-finding key, robust to trivial whitespace edits in the body."""
    normalized = " ".join(body.split())
    digest = hashlib.sha1(f"{path}:{line}:{normalized}".encode()).hexdigest()
    return digest[:12]


def reply_key(root_comment_id, body):
    normalized = " ".join(body.split())
    digest = hashlib.sha1(f"{root_comment_id}:{normalized}".encode()).hexdigest()
    return digest[:12]


def existing_markers(repo, pr):
    """Return (inline_keys, reply_keys, {(path,line): bool}) already present."""
    comments = gh(
        ["api", f"repos/{repo}/pulls/{pr}/comments", "--paginate"]
    ) or []
    inline_keys, reply_keys, threaded_lines = set(), set(), set()
    for c in comments:
        body = c.get("body", "")
        for m in INLINE_MARKER_RE.finditer(body):
            inline_keys.add(m.group(1))
        for m in REPLY_MARKER_RE.finditer(body):
            reply_keys.add(m.group(1))
        if INLINE_MARKER_RE.search(body) and c.get("path") and c.get("line"):
            threaded_lines.add((c["path"], c["line"]))
    return inline_keys, reply_keys, threaded_lines


def find_summary_comment(repo, pr):
    comments = gh(
        ["api", f"repos/{repo}/issues/{pr}/comments", "--paginate"]
    ) or []
    for c in comments:
        if SUMMARY_MARKER in c.get("body", ""):
            return c["id"]
    return None


def resolve_thread(thread_id):
    query = (
        "mutation($threadId:ID!){resolveReviewThread(input:{threadId:$threadId})"
        "{thread{isResolved}}}"
    )
    gh(["api", "graphql", "-f", f"query={query}", "-f", f"threadId={thread_id}"])


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


def cmd_fetch(args):
    print(json.dumps(fetch_threads(args.repo, args.pr), indent=2))


def cmd_post(args):
    with open(args.input) as fh:
        data = json.load(fh)

    findings = data.get("findings", []) or []
    general = data.get("general_findings", []) or []
    thread_actions = data.get("thread_actions", []) or []

    commentable = diff_commentable_lines(args.repo, args.pr)
    inline_keys, reply_keys, threaded_lines = existing_markers(args.repo, args.pr)

    # --- new inline comments ---
    new_comments, demoted, skipped_dup = [], [], 0
    for f in findings:
        if f.get("severity") not in VALID_SEVERITIES:
            f["severity"] = "should_fix"
        path, line, body = f.get("path"), f.get("line"), f.get("body", "")
        if (not path or not isinstance(line, int)
                or line not in commentable.get(path, set())):
            f["_loc"] = f"{path}:{line}" if path and line else (path or "")
            demoted.append(f)
            continue
        if (path, line) in threaded_lines:
            # A thread already exists here; re-engagement must go through
            # thread_actions (reply), not a duplicate top-level comment.
            skipped_dup += 1
            continue
        key = inline_key(path, line, body)
        if key in inline_keys:
            skipped_dup += 1
            continue
        marked = f"{render_finding_body(f)}\n\n<!-- claude-review:inline:{key} -->"
        new_comments.append(
            {"path": path, "line": line, "side": "RIGHT", "body": marked})

    if new_comments:
        gh(["api", f"repos/{args.repo}/pulls/{args.pr}/reviews",
            "--method", "POST", "--input", "-"],
           payload={"event": "COMMENT", "comments": new_comments})
        print(f"Posted {len(new_comments)} new inline comment(s).")
    else:
        print("No new inline comments to post.")
    if skipped_dup:
        print(f"Skipped {skipped_dup} inline finding(s) already present.")
    if demoted:
        print(f"Demoted {len(demoted)} off-diff finding(s) into the summary.")

    # --- thread replies and resolutions ---
    replied = resolved = 0
    for act in thread_actions:
        action = act.get("action")
        thread_id = act.get("thread_id")
        root_id = act.get("root_comment_id")
        if action == "reply" and root_id:
            body = (act.get("reply_body") or "").strip()
            if not body:
                continue
            key = reply_key(root_id, body)
            if key in reply_keys:
                continue  # already replied with this content
            marked = f"{body}\n\n<!-- claude-review:reply:{key} -->"
            gh(["api", f"repos/{args.repo}/pulls/{args.pr}/comments/{root_id}/replies",
                "--method", "POST", "--input", "-"],
               payload={"body": marked})
            replied += 1
        elif action == "resolve" and thread_id:
            if act.get("reply_body"):
                body = act["reply_body"].strip()
                key = reply_key(root_id, body) if root_id else None
                if root_id and key not in reply_keys:
                    marked = f"{body}\n\n<!-- claude-review:reply:{key} -->"
                    gh(["api",
                        f"repos/{args.repo}/pulls/{args.pr}/comments/{root_id}/replies",
                        "--method", "POST", "--input", "-"],
                       payload={"body": marked})
            # If resolving fails (e.g. missing permission), just warn —
            # not worth failing the whole post step over.
            try:
                resolve_thread(thread_id)
                resolved += 1
            except RuntimeError as exc:
                print(
                    f"warning: could not resolve thread {thread_id}: {exc}",
                    file=sys.stderr,
                )
        # action == "keep" (or unknown): do nothing
    if replied:
        print(f"Replied to {replied} thread(s).")
    if resolved:
        print(f"Resolved {resolved} thread(s).")

    # --- summary comment (always reflects latest state) ---
    summary_body = render_summary(data, general, demoted)
    summary_id = find_summary_comment(args.repo, args.pr)
    if summary_id is not None:
        gh(["api", f"repos/{args.repo}/issues/comments/{summary_id}",
            "--method", "PATCH", "--input", "-"],
           payload={"body": summary_body})
        print(f"Updated summary comment {summary_id}.")
    else:
        gh(["api", f"repos/{args.repo}/issues/{args.pr}/comments",
            "--method", "POST", "--input", "-"],
           payload={"body": summary_body})
        print("Created summary comment.")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    fp = sub.add_parser("fetch", help="dump our existing review threads as JSON")
    fp.add_argument("--repo", required=True, help="owner/repo")
    fp.add_argument("--pr", required=True)
    fp.set_defaults(func=cmd_fetch)

    pp = sub.add_parser("post", help="post findings JSON as review feedback")
    pp.add_argument("--repo", required=True, help="owner/repo")
    pp.add_argument("--pr", required=True)
    pp.add_argument("--input", required=True, help="path to findings JSON")
    pp.set_defaults(func=cmd_post)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"post_review.py: {exc}", file=sys.stderr)
        sys.exit(1)
