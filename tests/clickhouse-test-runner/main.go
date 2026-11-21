// Command clickhouse-test-runner imitates `clickhouse-client` well enough for
// the ClickHouse core test runner (`tests/clickhouse-test`) to drive it, and
// runs every query through clickhouse-go. It reads the result values that
// clickhouse-go decoded and writes them as `TabSeparated` text, so the
// upstream `.reference` files check the decoding of the driver.
//
// See README.md for how to run the upstream SQL tests with it.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

const protocolEnv = "CLICKHOUSE_TEST_RUNNER_PROTOCOL"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(argv []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(argv) > 0 && argv[0] == "extract-from-config" {
		return extractFromConfig(argv[1:], stdout)
	}

	log := newInvocationLog()
	defer log.flush()
	log.add("argv", strings.Join(redactPassword(argv), " "))

	opts, err := parseArgs(argv)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n\n%s", err, usageText)
		return 1
	}
	if opts.Help {
		if err := printUsage(stdout); err != nil {
			return 1
		}
		return 0
	}

	protocol, err := protocolFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	log.add("protocol", protocol.String())

	var text string
	if opts.Query != nil {
		text = *opts.Query
	} else {
		b, err := io.ReadAll(stdin)
		if err != nil {
			fmt.Fprintf(stderr, "Error: read stdin: %v\n", err)
			return 1
		}
		text = string(b)
	}
	if strings.TrimSpace(text) == "" {
		fmt.Fprintln(stderr, "Error: no query. Use --query or write the queries to stdin.")
		return 1
	}

	stmts := splitStatements(text)
	log.add("statements", fmt.Sprint(len(stmts)))
	if len(stmts) == 0 {
		return 0
	}

	out := bufio.NewWriter(stdout)
	r, err := newRunner(opts, protocol, out, log)
	if err != nil {
		fmt.Fprintf(stderr, "Error: %v\n", err)
		return 1
	}
	runErr := r.run(context.Background(), stmts)
	flushErr := out.Flush()
	closeErr := r.close()
	if runErr == nil {
		runErr = errors.Join(flushErr, closeErr)
	}
	if runErr != nil {
		log.add("error", runErr.Error())
		return reportError(stderr, runErr)
	}
	return 0
}

// reportError writes err to stderr and returns the exit code. As in
// `clickhouse-client`, the exit code is the error code modulo 256 when it is
// a server error.
func reportError(stderr io.Writer, err error) int {
	var (
		exc  *proto.Exception
		hint *hintError
		code int32
	)
	switch {
	case errors.As(err, &hint):
		fmt.Fprintf(stderr, "Error: %s\n", hint.msg)
		code = hint.code
	case errors.As(err, &exc):
		fmt.Fprintf(stderr, "Received exception from server:\n%s\n", formatServerError(exc))
		code = exc.Code
	default:
		fmt.Fprintf(stderr, "Error: %v\n", err)
	}
	if code%256 != 0 {
		return int(code % 256)
	}
	return 1
}

func protocolFromEnv() (clickhouse.Protocol, error) {
	switch v := os.Getenv(protocolEnv); v {
	case "", "native":
		return clickhouse.Native, nil
	case "http":
		return clickhouse.HTTP, nil
	default:
		return 0, fmt.Errorf("unknown %s %q: use native or http", protocolEnv, v)
	}
}

// extractFromConfig answers `clickhouse extract-from-config --key <key>`,
// which `tests/clickhouse-test` runs to read the server config. Only
// `listen_host` has an answer; for other keys the output is empty, as for a
// key that is not in the config.
func extractFromConfig(argv []string, stdout io.Writer) int {
	var key string
	for i := 0; i < len(argv); i++ {
		switch a := argv[i]; {
		case a == "--key" && i+1 < len(argv):
			i++
			key = argv[i]
		case strings.HasPrefix(a, "--key="):
			key = strings.TrimPrefix(a, "--key=")
		}
	}
	if key == "listen_host" {
		if _, err := fmt.Fprintln(stdout, "127.0.0.1"); err != nil {
			return 1
		}
	}
	return 0
}
