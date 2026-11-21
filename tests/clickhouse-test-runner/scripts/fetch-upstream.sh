#!/usr/bin/env bash
# Gets the upstream ClickHouse tests of the pinned server version, and
# generates errorcodes.go from the same checkout.
#
# Usage: scripts/fetch-upstream.sh [<version or ref>]
#
# Without an argument, the script reads the version from ../clickhouse-version
# (for example 26.10.4.207) and checks out its release tag
# (v26.10.4.207-stable). The `.reference` files of that tag were made by the
# same server build. A version as the argument selects its release tag; any
# other argument is used as a ref (for example `master`).
#
# The checkout is sparse and shallow. It goes to UPSTREAM_CLICKHOUSE_DIR
# (default: .upstream/ClickHouse).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
UPSTREAM_CLICKHOUSE_DIR="${UPSTREAM_CLICKHOUSE_DIR:-${RUNNER_DIR}/.upstream/ClickHouse}"
default_repo_url="https://github.com/ClickHouse/ClickHouse.git"
UPSTREAM_REPO_URL="${UPSTREAM_REPO_URL:-${default_repo_url}}"

ref="${1:-}"
if [[ -z "${ref}" ]]; then
  ref="$("${SCRIPT_DIR}/pinned-version.sh")"
fi
if [[ "${ref}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  ref="v${ref}-stable"
fi
echo "Upstream ref: ${ref}" >&2

if [[ ! -d "${UPSTREAM_CLICKHOUSE_DIR}/.git" ]]; then
  mkdir -p "${UPSTREAM_CLICKHOUSE_DIR}"
  git -C "${UPSTREAM_CLICKHOUSE_DIR}" init -q
  git -C "${UPSTREAM_CLICKHOUSE_DIR}" remote add origin "${UPSTREAM_REPO_URL}"
fi
# Not cone mode: the list has files, and cone mode takes only directories.
git -C "${UPSTREAM_CLICKHOUSE_DIR}" sparse-checkout set --no-cone \
  /tests/clickhouse-test /tests/queries/ /tests/config/ /tests/ci/ /tests/performance/ \
  /docker/test/util/ /src/Common/ErrorCodes.cpp
# A tag and a branch both resolve here, so the same command takes `master`.
if ! git -C "${UPSTREAM_CLICKHOUSE_DIR}" fetch -q --depth 1 --filter=blob:none origin "${ref}"; then
  echo "Error: cannot fetch ${ref} from ${UPSTREAM_REPO_URL}." >&2
  exit 1
fi
git -C "${UPSTREAM_CLICKHOUSE_DIR}" checkout -q --force FETCH_HEAD
chmod +x "${UPSTREAM_CLICKHOUSE_DIR}/tests/clickhouse-test"
echo "${ref}" > "${UPSTREAM_CLICKHOUSE_DIR}/.upstream-ref"

"${SCRIPT_DIR}/gen-error-codes.sh" "${UPSTREAM_CLICKHOUSE_DIR}"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "ref=${ref}" >> "${GITHUB_OUTPUT}"
fi
