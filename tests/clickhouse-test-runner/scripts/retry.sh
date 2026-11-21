# Sourced by run-upstream-tests.sh. retry_test.go tests it with fake runners.
#
# run_with_retries <retries> <runner arguments>... runs the tests in the
# array `tests` with the command in the array `runner`, then runs the failed
# tests again, at most <retries> times. It needs build_selectors from
# selectors.sh. It returns the exit code of the last attempt.
#
# A known client bug, [native-pool-race-new-connection] in allowlist.txt,
# makes about 40 random tests with SET fail per native run. A test that
# passes on a retry is reported as a warning; a test that fails on every
# attempt fails the run.
#
# A retry can only replace an attempt that completed: every test of the
# attempt has a result line, and the runner exited with 0 (all passed) or
# 1 (tests failed). Otherwise (the runner was killed, or it stopped early)
# the tests without a result never ran, so the run fails at once.

# run_attempt <log> <runner arguments>... runs the runner on the current
# selectors and sets attempt_status, attempt_failed, attempt_passed and
# attempt_missing (the expected tests without a result line).
run_attempt() {
  local log="$1" t
  shift
  set +e
  "${runner[@]}" --max-failures-chain 1000000 "$@" -- "${selectors[@]}" 2>&1 | tee "${log}"
  attempt_status=${PIPESTATUS[0]}
  set -e
  attempt_failed=()
  while IFS= read -r t; do
    attempt_failed+=("${t}")
  done < <(sed -nE 's/^\[[ 0-9]+\/[ 0-9]+\] ([^ :]+): +\[ FAIL \].*/\1/p' "${log}" | sort -u)
  attempt_passed="$(sed -nE 's/^\[[ 0-9]+\/[ 0-9]+\] ([^ :]+): +\[ OK \].*/\1/p' "${log}" | sort -u | tr '\n' ' ')"
  local seen
  seen="$(sed -nE 's/^\[[ 0-9]+\/[ 0-9]+\] ([^ :]+): +\[ [A-Z ]+ \].*/\1/p' "${log}" | sort -u)"
  attempt_missing=()
  for t in "${attempt_tests[@]}"; do
    if ! grep -qxF -- "${t}" <<< "${seen}"; then
      attempt_missing+=("${t}")
    fi
  done
}

# attempt_complete reports an incomplete attempt and returns 1 for it.
attempt_complete() {
  if [[ ${attempt_status} -ne 0 && ${attempt_status} -ne 1 ]]; then
    echo "Error: the runner exited with code ${attempt_status}, so the run did not complete. No retry." >&2
    return 1
  fi
  if [[ ${#attempt_missing[@]} -gt 0 ]]; then
    echo "Error: ${#attempt_missing[@]} test(s) have no result, so the run did not complete. No retry. Without a result: ${attempt_missing[*]}" >&2
    return 1
  fi
  return 0
}

run_with_retries() {
  local retries="$1" log attempt=0 passed_on_retry="" status
  shift
  log="$(mktemp)"
  attempt_tests=("${tests[@]}")
  build_selectors "${attempt_tests[@]}"
  run_attempt "${log}" "$@"
  while (( attempt_status != 0 )); do
    if ! attempt_complete; then
      status=${attempt_status}
      rm -f "${log}"
      return $(( status == 0 ? 1 : status ))
    fi
    if [[ ${#attempt_failed[@]} -eq 0 ]]; then
      echo "Error: the run failed (exit code ${attempt_status}), but no test failed. See the output above." >&2
      rm -f "${log}"
      return "${attempt_status}"
    fi
    if (( attempt >= retries )); then
      break
    fi
    attempt=$(( attempt + 1 ))
    echo "Retry ${attempt}/${retries}: ${#attempt_failed[@]} failed test(s): ${attempt_failed[*]}" >&2
    attempt_tests=("${attempt_failed[@]}")
    build_selectors "${attempt_tests[@]}"
    run_attempt "${log}" "$@"
    passed_on_retry="${passed_on_retry}${attempt_passed}"
  done
  # The last attempt must have completed too.
  if (( attempt_status == 0 )) && ! attempt_complete; then
    rm -f "${log}"
    return 1
  fi
  rm -f "${log}"

  if [[ -n "${passed_on_retry}" ]]; then
    local msg="These tests failed and then passed on a retry (the known pool race, [native-pool-race-new-connection] in allowlist.txt): ${passed_on_retry}"
    if [[ -n "${GITHUB_ACTIONS:-}" ]]; then echo "::warning::${msg}"; else echo "Warning: ${msg}" >&2; fi
  fi
  return "${attempt_status}"
}
