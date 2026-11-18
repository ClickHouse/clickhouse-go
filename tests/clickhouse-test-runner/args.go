package main

import (
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// options holds the `clickhouse-client` command line, as far as
// `tests/clickhouse-test` and the `.sh` tests use it.
type options struct {
	Host     string
	Port     int // 0 when not given
	User     string
	Password string
	Database string
	Secure   bool
	Query    *string
	Help     bool

	// Settings are the server settings to apply to the session.
	Settings map[string]string
	// SettingOrder is the order in which Settings were given.
	SettingOrder []string
	// Params are the query parameters from `--param_<name>=<value>`.
	Params map[string]string
}

// serverSettings are the command-line settings that the shim sends to the
// server. `tests/clickhouse-test` also adds many randomized settings to the
// command line. The shim drops them: they only change performance-related
// paths in the server, and some of them are client-only.
var serverSettings = map[string]bool{
	"log_comment":        true,
	"max_insert_threads": true,
	"max_threads":        true,
	"send_logs_level":    true,
}

type optionSpec struct {
	long   string
	short  string
	hasArg bool
}

var knownOptions = []optionSpec{
	{long: "host", short: "h", hasArg: true},
	{long: "port", hasArg: true},
	{long: "user", short: "u", hasArg: true},
	{long: "password", hasArg: true},
	{long: "database", short: "d", hasArg: true},
	{long: "query", short: "q", hasArg: true},
	{long: "secure", short: "s"},
	{long: "no-secure"},
	// As in clickhouse-client: `-n` is `--multiquery`, `-m` is
	// `--multiline`. The shim always runs every statement of the input.
	{long: "multiquery", short: "n"},
	{long: "multi-query"},
	{long: "multiline", short: "m"},
	{long: "help"},
	// Accepted and ignored.
	{long: "config-file", short: "C", hasArg: true},
	{long: "client_logs_file", hasArg: true},
	{long: "allow_repeated_settings"},
	{long: "fake-drop"},
}

var negativeNumber = regexp.MustCompile(`^-(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$`)

func findOption(long, short string) (optionSpec, bool) {
	for _, o := range knownOptions {
		if (long != "" && o.long == long) || (short != "" && o.short == short) {
			return o, true
		}
	}
	return optionSpec{}, false
}

func parseArgs(argv []string) (*options, error) {
	opts := &options{
		Host:     "localhost",
		User:     "default",
		Database: "default",
		Settings: map[string]string{},
		Params:   map[string]string{},
	}
	set := func(name, value string) error {
		switch name {
		case "host":
			opts.Host = value
		case "port":
			p, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("bad --port value %q: %w", value, err)
			}
			opts.Port = p
		case "user":
			opts.User = value
		case "password":
			opts.Password = value
		case "database":
			opts.Database = value
		case "query":
			v := value
			opts.Query = &v
		case "secure":
			opts.Secure = true
		case "no-secure":
			opts.Secure = false
		case "help":
			opts.Help = true
		}
		return nil
	}

	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		switch {
		case arg == "--":
			return opts, nil
		case strings.HasPrefix(arg, "--"):
			name, value, hasValue := strings.Cut(arg[2:], "=")
			if name == "" {
				continue
			}
			if o, ok := findOption(name, ""); ok {
				if !o.hasArg {
					if err := set(o.long, ""); err != nil {
						return nil, err
					}
					continue
				}
				if !hasValue {
					if i+1 >= len(argv) {
						return nil, fmt.Errorf("option --%s needs a value", name)
					}
					i++
					value = argv[i]
				}
				if err := set(o.long, value); err != nil {
					return nil, err
				}
				continue
			}
			// Any other long option is a setting (`--max_threads=1`) or a
			// query parameter (`--param_x=1`). The value is optional.
			if !hasValue && i+1 < len(argv) {
				next := argv[i+1]
				if !strings.HasPrefix(next, "-") || negativeNumber.MatchString(next) {
					value, hasValue = next, true
					i++
				}
			}
			if !hasValue {
				value = "1"
			}
			if p, ok := strings.CutPrefix(name, "param_"); ok {
				opts.Params[p] = value
				continue
			}
			setting := strings.ReplaceAll(name, "-", "_")
			if serverSettings[setting] {
				if _, seen := opts.Settings[setting]; !seen {
					opts.SettingOrder = append(opts.SettingOrder, setting)
				}
				opts.Settings[setting] = value
			}
		case strings.HasPrefix(arg, "-") && len(arg) > 1:
			o, ok := findOption("", arg[1:])
			if !ok {
				// Unknown short options are ignored, as the other
				// language ports do.
				continue
			}
			if !o.hasArg {
				if err := set(o.long, ""); err != nil {
					return nil, err
				}
				continue
			}
			if i+1 >= len(argv) {
				return nil, fmt.Errorf("option %s needs a value", arg)
			}
			i++
			if err := set(o.long, argv[i]); err != nil {
				return nil, err
			}
		}
		// Positional arguments are ignored.
	}
	return opts, nil
}

const usageText = `usage: clickhouse-client [options]

A clickhouse-client imitation that runs queries through clickhouse-go, so that
the ClickHouse tests/clickhouse-test runner can drive it. If --query is not
given, the queries are read from stdin. Every ";"-separated statement runs.

Options:
  -h, --host HOST          Server host (default: localhost)
      --port PORT          Server port. The native TCP port of the test runner
                           (CLICKHOUSE_PORT_TCP) maps to CLICKHOUSE_PORT_HTTP
                           when the protocol is http.
  -u, --user USER          User name (default: default)
      --password PASSWORD  Password (default: empty)
  -d, --database DB        Database (default: default)
  -q, --query SQL          Queries to run
  -s, --secure             Use TLS
      --param_NAME=VALUE   Query parameter
      --SETTING=VALUE      log_comment, max_insert_threads, max_threads and
                           send_logs_level go to the server; other settings
                           are ignored
  -n, --multiquery         Ignored
  -m, --multiline          Ignored
      --help               Show this help

Environment variables:
  CLICKHOUSE_TEST_RUNNER_PROTOCOL  native (default) or http
  CLICKHOUSE_CLIENT_CLI_LOG        Log file for troubleshooting
`

func printUsage(w io.Writer) error {
	_, err := io.WriteString(w, usageText)
	return err
}
