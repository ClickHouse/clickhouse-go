# Sourced by run-upstream-tests.sh and run-with-real-client.sh.
#
# build_selectors <test>... sets the array `selectors` to anchored regular
# expressions that select exactly the given tests.
#
# tests/clickhouse-test reads each test argument as a regular expression and
# selects every file that it matches anywhere in the name, so `00396_uuid`
# would also select `00396_uuid_v7`. An anchored alternation selects exactly
# the listed tests, and is much faster than one expression per test. Linux
# limits one argument to 128 KiB, so the alternation is split into several
# arguments of at most selector_max_bytes.
selector_max_bytes=65536

build_selectors() {
  selectors=()
  local chunk="" t
  for t in "$@"; do
    t="${t//./\\.}"
    if [[ -n "${chunk}" && $(( ${#chunk} + ${#t} + 32 )) -gt ${selector_max_bytes} ]]; then
      selectors+=("^(${chunk})\\.(sql|sql\\.j2)\$")
      chunk=""
    fi
    chunk="${chunk:+${chunk}|}${t}"
  done
  if [[ -n "${chunk}" ]]; then
    selectors+=("^(${chunk})\\.(sql|sql\\.j2)\$")
  fi
}
