package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runRetries runs run_with_retries of scripts/retry.sh with a fake runner,
// for the tests a, b and c and 2 retries. calls is the bash body of the
// fake runner for each call (1, 2, ...): it prints result lines and exits.
// It returns the exit code, the output, and the number of runner calls.
func runRetries(t *testing.T, calls map[int]string) (int, string, int) {
	t.Helper()
	dir := t.TempDir()
	counter := filepath.Join(dir, "calls")
	var body strings.Builder
	body.WriteString("#!/usr/bin/env bash\n")
	body.WriteString("n=$(( $(cat " + counter + " 2>/dev/null || echo 0) + 1 )); echo $n > " + counter + "\n")
	body.WriteString("ok() { echo \"[ 1 / 3] $1:      [ OK ] 0.1 sec.\"; }\n")
	body.WriteString("fail() { echo \"[ 1 / 3] $1:      [ FAIL ] 0.1 sec.\"; }\n")
	body.WriteString("case $n in\n")
	for n, script := range calls {
		body.WriteString(strconv.Itoa(n) + ") " + script + " ;;\n")
	}
	body.WriteString("*) echo \"unexpected call $n\"; exit 99 ;;\nesac\n")
	fake := filepath.Join(dir, "fake-runner.sh")
	require.NoError(t, os.WriteFile(fake, []byte(body.String()), 0o755))

	scripts, err := filepath.Abs("scripts")
	require.NoError(t, err)
	cmd := exec.Command("bash", "-c", `set -euo pipefail
source "$1/selectors.sh"
source "$1/retry.sh"
runner=("$2")
tests=(a b c)
status=0
run_with_retries 2 -j 4 || status=$?
exit "${status}"`, "bash", scripts, fake)
	out, err := cmd.CombinedOutput()
	code := 0
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		code = exitErr.ExitCode()
	} else {
		require.NoError(t, err)
	}
	n := 0
	if b, err := os.ReadFile(counter); err == nil {
		n, err = strconv.Atoi(strings.TrimSpace(string(b)))
		require.NoError(t, err)
	}
	return code, string(out), n
}

func TestRetryAllPass(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{1: "ok a; ok b; ok c; exit 0"})
	assert.Equal(t, 0, code, out)
	assert.Equal(t, 1, calls)
}

func TestRetryFlakyTestPasses(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{
		1: "ok a; fail b; ok c; exit 1",
		2: "ok b; exit 0",
	})
	assert.Equal(t, 0, code, out)
	assert.Equal(t, 2, calls)
	assert.Contains(t, out, "passed on a retry")
}

func TestRetryBrokenTestFails(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{
		1: "ok a; fail b; ok c; exit 1",
		2: "fail b; exit 1",
		3: "fail b; exit 1",
	})
	assert.Equal(t, 1, code, out)
	assert.Equal(t, 3, calls)
}

// TestRetryKilledRunIsNotRetried: the runner prints a failure for a and is
// killed before b and c run. A retry of a alone must not make the run pass.
func TestRetryKilledRunIsNotRetried(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{
		1: "fail a; exit 137",
		2: "ok a; exit 0",
	})
	assert.Equal(t, 137, code, out)
	assert.Equal(t, 1, calls)
	assert.Contains(t, out, "did not complete")
}

// TestRetryIncompleteRunIsNotRetried: the runner exits with 1, but b has no
// result line, so b never ran.
func TestRetryIncompleteRunIsNotRetried(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{
		1: "fail a; ok c; exit 1",
		2: "ok a; exit 0",
	})
	assert.Equal(t, 1, code, out)
	assert.Equal(t, 1, calls)
	assert.Contains(t, out, "Without a result: b")
}

// TestRetryKilledRetryFails: the retry itself is killed.
func TestRetryKilledRetryFails(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{
		1: "ok a; fail b; ok c; exit 1",
		2: "exit 137",
	})
	assert.Equal(t, 137, code, out)
	assert.Equal(t, 2, calls)
}

// TestRetrySuccessWithoutAllResultsFails: exit code 0, but a test has no
// result line.
func TestRetrySuccessWithoutAllResultsFails(t *testing.T) {
	code, out, calls := runRetries(t, map[int]string{1: "ok a; ok b; exit 0"})
	assert.Equal(t, 1, code, out)
	assert.Equal(t, 1, calls)
	assert.Contains(t, out, "Without a result: c")
}
