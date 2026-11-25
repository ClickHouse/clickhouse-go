#!/usr/bin/env bash
# Prints the pinned ClickHouse server version from ../clickhouse-version.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
file="${SCRIPT_DIR}/../clickhouse-version"
version="$(grep -v '^[[:space:]]*#' "${file}" | tr -d '[:space:]')"
if ! [[ "${version}" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Error: ${file} must contain one version such as 26.10.4.207 (got: '${version}')." >&2
  exit 1
fi
echo "${version}"
