package main

import (
	"os"
	"strings"
	"time"
)

const logPathEnv = "CLICKHOUSE_CLIENT_CLI_LOG"

// invocationLog collects troubleshooting lines for one invocation and
// appends them to the file named by CLICKHOUSE_CLIENT_CLI_LOG. Without that
// variable, it does nothing. The lines are written in one write, so the
// lines of parallel invocations do not mix.
type invocationLog struct {
	path  string
	lines strings.Builder
}

func newInvocationLog() *invocationLog {
	l := &invocationLog{path: os.Getenv(logPathEnv)}
	l.add("=== clickhouse-client invocation", time.Now().UTC().Format(time.RFC3339Nano))
	return l
}

func (l *invocationLog) add(key, value string) {
	if l.path == "" {
		return
	}
	l.lines.WriteString(key)
	l.lines.WriteByte('=')
	l.lines.WriteString(strings.ReplaceAll(value, "\n", "\\n"))
	l.lines.WriteByte('\n')
}

// flush appends the lines to the log file. A log that cannot be written must
// not change the result of a test, so a write error is ignored here; the
// missing lines in the log show it.
func (l *invocationLog) flush() {
	if l.path == "" {
		return
	}
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	if _, err := f.WriteString(l.lines.String()); err != nil {
		return
	}
}
