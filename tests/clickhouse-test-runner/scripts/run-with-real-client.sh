#!/usr/bin/env bash
# Runs upstream tests through the REAL clickhouse-client in the server
# container, not through clickhouse-go. Use it as the reference when you
# triage a failure: if a test fails here too, the cause is the environment
# (server config, missing cluster, ...), not clickhouse-go or the shim.
#
# Usage: scripts/run-with-real-client.sh [<extra tests/clickhouse-test arguments>...]
#
# It reads the same UPSTREAM_CLICKHOUSE_DIR and UPSTREAM_TEST_LIST as
# run-upstream-tests.sh. CLICKHOUSE_CONTAINER is the name of the server
# container (default: clickhouse, the name that `make up` uses).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

UPSTREAM_CLICKHOUSE_DIR="${UPSTREAM_CLICKHOUSE_DIR:-${RUNNER_DIR}/.upstream/ClickHouse}"
UPSTREAM_TEST_LIST="${UPSTREAM_TEST_LIST:-${RUNNER_DIR}/allowlist.txt}"
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-clickhouse}"

if ! docker exec "${CLICKHOUSE_CONTAINER}" clickhouse-client --query "SELECT 1" > /dev/null; then
  echo "Error: cannot run clickhouse-client in container '${CLICKHOUSE_CONTAINER}'. Set CLICKHOUSE_CONTAINER." >&2
  exit 1
fi

# A clickhouse binary that forwards to the real one in the container.
BIN_DIR="${RUNNER_DIR}/.bin-real-client"
mkdir -p "${BIN_DIR}"
cat > "${BIN_DIR}/clickhouse" <<EOF
#!/usr/bin/env bash
if [[ "\${1:-}" == "extract-from-config" ]]; then
  for a in "\$@"; do
    [[ "\$a" == "listen_host" || "\$a" == "--key=listen_host" ]] && echo 127.0.0.1
  done
  exit 0
fi
args=()
for a in "\$@"; do
  case "\$a" in
    # The runner's host, port and log file are paths and ports on this machine.
    client|--host=*|--port=*|--client_logs_file=*) ;;
    *) args+=("\$a") ;;
  esac
done
exec docker exec -i "${CLICKHOUSE_CONTAINER}" clickhouse-client "\${args[@]}"
EOF
chmod +x "${BIN_DIR}/clickhouse"
ln -sf clickhouse "${BIN_DIR}/clickhouse-client"

tests=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  line="${line%%#*}"
  line="${line#"${line%%[![:space:]]*}"}"
  line="${line%"${line##*[![:space:]]}"}"
  [[ -z "${line}" ]] && continue
  tests+=("${line}")
done < "${UPSTREAM_TEST_LIST}"
if [[ ${#tests[@]} -eq 0 ]]; then
  echo "Error: no tests selected from ${UPSTREAM_TEST_LIST}." >&2
  exit 1
fi
# shellcheck source=selectors.sh
source "${SCRIPT_DIR}/selectors.sh"
build_selectors "${tests[@]}"

export PATH="${BIN_DIR}:${PATH}"
cd "${UPSTREAM_CLICKHOUSE_DIR}"
exec python3 tests/clickhouse-test --no-stateful --no-random-settings --no-random-merge-tree-settings "$@" -- "${selectors[@]}"
