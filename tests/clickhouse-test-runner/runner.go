package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

const syntaxErrorCode = 62

// runner runs the statements of one shim invocation on one session.
//
// Session state (`SET`, `USE`) must survive from one statement to the next,
// as in `clickhouse-client`:
//   - native: a session is a TCP connection, so the pool has one connection.
//     clickhouse-go closes a connection after any error, including a server
//     exception that a test hint expects. The runner then opens a new
//     connection and runs the `USE` and `SET` statements of the session
//     again. Other session state, such as temporary tables, is lost.
//   - http: every request has the same `session_id`. The server keeps the
//     session after an error.
//
// The shim sends nothing that clickhouse-client does not send, because some
// tests read their own queries from `system.query_log`.
type runner struct {
	opts     *options
	protocol clickhouse.Protocol
	conn     driver.Conn
	out      *bufio.Writer
	log      *invocationLog

	// database is the current database of the session.
	database string
	// settings are the command-line settings that go with each query, as
	// clickhouse-client sends them. A `SET` of the same setting in the test
	// removes it from here, because the session then holds the value.
	settings clickhouse.Settings
	// dialect is the value of the last `SET dialect`.
	dialect string
	// implicit is set by `SET implicit_select = 1`, with which an expression
	// alone (`1 + 2`) is a query.
	implicit bool
	// sessionInit are the `USE` and `SET` statements of the test. They
	// rebuild the session state on a new native connection.
	sessionInit []string
	sessionID   string
	needsReset  bool
	// result holds the output of the current statement until the runner
	// knows whether it echoes the statement first.
	result bytes.Buffer
}

func newRunner(opts *options, protocol clickhouse.Protocol, out *bufio.Writer, log *invocationLog) (*runner, error) {
	r := &runner{
		opts:     opts,
		protocol: protocol,
		out:      out,
		log:      log,
		database: opts.Database,
		settings: clickhouse.Settings{},
	}
	for name, value := range opts.Settings {
		r.settings[name] = value
	}
	if protocol == clickhouse.HTTP {
		id, err := newSessionID()
		if err != nil {
			return nil, err
		}
		r.sessionID = id
		log.add("session_id", id)
	}
	return r, nil
}

func newSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("make session_id: %w", err)
	}
	return "clickhouse-go-test-runner-" + hex.EncodeToString(b), nil
}

func (r *runner) address() string {
	port := r.opts.Port
	if r.protocol == clickhouse.HTTP {
		port = httpPort(port, r.opts.Secure)
	} else if port == 0 {
		port = 9000
		if r.opts.Secure {
			port = 9440
		}
	}
	return net.JoinHostPort(r.opts.Host, strconv.Itoa(port))
}

// httpPort maps the port that `tests/clickhouse-test` gives (the native TCP
// port, from CLICKHOUSE_PORT_TCP) to the HTTP port that the runner exports.
// Any other port is used as given.
func httpPort(port int, secure bool) int {
	nativeEnv, httpEnv, nativeDefault, httpDefault := "CLICKHOUSE_PORT_TCP", "CLICKHOUSE_PORT_HTTP", 9000, 8123
	if secure {
		nativeEnv, httpEnv, nativeDefault, httpDefault = "CLICKHOUSE_PORT_TCP_SECURE", "CLICKHOUSE_PORT_HTTPS", 9440, 8443
	}
	native := envInt(nativeEnv, nativeDefault)
	if port != 0 && port != native {
		return port
	}
	return envInt(httpEnv, httpDefault)
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

func (r *runner) open() error {
	opt := &clickhouse.Options{
		Protocol: r.protocol,
		Addr:     []string{r.address()},
		Auth: clickhouse.Auth{
			Username: r.opts.User,
			Password: r.opts.Password,
			// Native: the handshake selects the database; after a
			// reconnection, the `USE` statements of sessionInit select the
			// current one again. HTTP: clickhouse-go sends this database with
			// each request, so useDatabase opens the connection again.
			Database: r.database,
		},
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	}
	if r.sessionID != "" {
		opt.Settings = clickhouse.Settings{"session_id": r.sessionID}
	}
	if r.opts.Secure {
		opt.TLS = &tls.Config{ServerName: r.opts.Host}
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		return fmt.Errorf("open %s connection to %s: %w", r.protocol, r.address(), err)
	}
	r.conn = conn
	r.log.add("address", r.address())
	return nil
}

func (r *runner) close() error {
	if r.conn == nil {
		return nil
	}
	err := r.conn.Close()
	r.conn = nil
	return err
}

func (r *runner) context(ctx context.Context) context.Context {
	var opts []clickhouse.QueryOption
	if len(r.settings) > 0 {
		settings := make(clickhouse.Settings, len(r.settings))
		for k, v := range r.settings {
			settings[k] = v
		}
		opts = append(opts, clickhouse.WithSettings(settings))
	}
	if len(r.opts.Params) > 0 {
		params := make(clickhouse.Parameters, len(r.opts.Params))
		for k, v := range r.opts.Params {
			params[k] = v
		}
		opts = append(opts, clickhouse.WithParameters(params))
	}
	if len(opts) == 0 {
		return ctx
	}
	return clickhouse.Context(ctx, opts...)
}

// initSession runs the statements that rebuild the session state on a new
// native connection.
func (r *runner) initSession(ctx context.Context) error {
	for _, q := range r.sessionInit {
		r.log.add("session_init", q)
		if err := r.conn.Exec(r.context(ctx), q); err != nil {
			return fmt.Errorf("initialize session with %q: %w", q, err)
		}
	}
	r.needsReset = false
	return nil
}

// run runs the statements in order. It stops at the first statement whose
// result does not match its test hint, like `clickhouse-client`.
func (r *runner) run(ctx context.Context, stmts []statement) error {
	if err := r.open(); err != nil {
		return err
	}
	for _, st := range stmts {
		if err := r.runOne(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (r *runner) runOne(ctx context.Context, st statement) error {
	if st.Hint.ParseError != nil {
		r.echo(st)
		return st.Hint.ParseError
	}
	if r.needsReset {
		if err := r.initSession(ctx); err != nil {
			return err
		}
	}

	r.log.add("query", st.SQL)
	r.result.Reset()
	err := r.execute(ctx, st.SQL)
	if err != nil && r.protocol == clickhouse.Native {
		// clickhouse-go closed the connection.
		r.needsReset = true
	}

	var exc *proto.Exception
	isExc := errors.As(err, &exc)
	// clickhouse-client parses a statement before it echoes it, so it does
	// not echo a statement with a syntax error.
	if !isExc || exc.Code != syntaxErrorCode {
		r.echo(st)
	}
	// Write errors are sticky in bufio.Writer; Flush reports them.
	r.out.Write(r.result.Bytes())
	if err := r.out.Flush(); err != nil {
		return fmt.Errorf("write stdout: %w", err)
	}

	switch {
	case st.Hint.expectsError() && err == nil:
		return &hintError{msg: fmt.Sprintf("expected %s, but the query succeeded (query: %s)", st.Hint.label(), st.Full)}
	case st.Hint.expectsError() && isExc:
		if !st.Hint.matches(exc.Code) {
			return &hintError{msg: fmt.Sprintf("expected %s, but got %s (query: %s)", st.Hint.label(), formatServerError(exc), st.Full), code: exc.Code}
		}
		r.log.add("expected_error_matched", st.Hint.label())
		return nil
	case err != nil:
		return err
	}
	return r.afterSuccess(st.SQL)
}

// echo writes the statement for the `echo` test hints.
func (r *runner) echo(st statement) {
	if st.Hint.echoThis {
		r.out.WriteString(st.Full)
		r.out.WriteByte('\n')
	}
}

// afterSuccess updates the session state after a `USE` or `SET` statement.
func (r *runner) afterSuccess(sql string) error {
	switch leadingKeyword(sql) {
	case "USE":
		db, ok := parseUseDatabase(sql, r.opts.Params)
		if !ok {
			return fmt.Errorf("clickhouse-test-runner: cannot read the database name of %q", sql)
		}
		r.sessionInit = append(r.sessionInit, sql)
		return r.useDatabase(db)
	case "SET":
		r.sessionInit = append(r.sessionInit, sql)
		for name, value := range parseSetAssignments(sql) {
			delete(r.settings, name)
			switch name {
			case "dialect":
				r.dialect = value
			case "implicit_select":
				r.implicit = value != "0" && !strings.EqualFold(value, "false")
			}
		}
	}
	return nil
}

// useDatabase records the new current database. Over HTTP, it opens the
// connection again with the same session_id: clickhouse-go sends the
// database of the connection with each request, and that database replaces
// the current database of the session.
func (r *runner) useDatabase(db string) error {
	r.database = db
	if r.protocol != clickhouse.HTTP {
		return nil
	}
	if err := r.close(); err != nil {
		return err
	}
	return r.open()
}

// hintError reports a result that does not match the test hint.
type hintError struct {
	msg  string
	code int32
}

func (e *hintError) Error() string { return e.msg }

func (r *runner) execute(ctx context.Context, sql string) error {
	ctx = r.context(ctx)
	if r.otherDialect(sql) && r.protocol == clickhouse.HTTP {
		// Over HTTP the server parses the request body as one query, and
		// the parsers of other dialects do not accept a trailing semicolon.
		sql = strings.TrimSuffix(strings.TrimRight(sql, " \t\r\n"), ";")
	}
	if !returnsRows(sql) && !r.otherDialect(sql) && !r.implicitSelect(sql) {
		return r.conn.Exec(ctx, sql)
	}
	query, format := sql, outputFormat{Name: "TabSeparated", Supported: true}
	if !r.otherDialect(sql) {
		// The FORMAT clause is SQL; other dialects can use the word
		// differently (the `format` pipe of LogsQL).
		query, format = splitOutputFormat(sql)
	}
	if !format.Supported {
		return fmt.Errorf("output format %s is not supported: clickhouse-go reads results as Native, and the shim writes them only as TabSeparated", format.Name)
	}
	explainNull := format.Discard && leadingKeyword(sql) == "EXPLAIN"
	if explainNull {
		// With ignore_format_null_for_explain (on by default), the server
		// ignores `FORMAT Null` for EXPLAIN and sends the plan. The server
		// decides, so write the rows that it sends.
		format = outputFormat{Name: "TabSeparated", Supported: true}
	}
	if r.protocol == clickhouse.Native || format.Discard || explainNull {
		// Send the text as clickhouse-client does; tests that read
		// system.query_log see it. Over native the server ignores the FORMAT
		// clause (the client formats the result). With `FORMAT Null`, the
		// server sends no rows over either protocol.
		query = sql
	}
	rows, err := r.conn.Query(ctx, query)
	//nolint:errorlint // Only the bare io.EOF sentinel, not a wrapped one.
	if err == io.EOF && r.protocol == clickhouse.Native {
		// The statement returned no result block, for example
		// `WITH ... INSERT`. For the ServerEndOfStream packet, clickhouse-go
		// returns the bare io.EOF, and it closes the connection. A read
		// failure (a disconnect, a truncated block) wraps io.EOF with %w; it
		// must stay an error, or a failed query looks like an empty result.
		r.needsReset = true
		return nil
	}
	if err != nil {
		return err
	}
	writeErr := r.writeRows(rows, format)
	closeErr := rows.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	return rows.Err()
}

// otherDialect tells if the session uses another query language than SQL
// (`SET dialect = 'kusto'`, 'prql', ...). The shim cannot see if a statement
// in such a language returns rows, so it uses Query for it.
func (r *runner) otherDialect(sql string) bool {
	return r.dialect != "" && !strings.EqualFold(r.dialect, "clickhouse") && leadingKeyword(sql) != "SET"
}

// implicitSelect tells if the statement is an expression alone, which
// `SET implicit_select = 1` makes a query.
func (r *runner) implicitSelect(sql string) bool {
	if !r.implicit {
		return false
	}
	// A first word followed by `(` is a function call, such as
	// `replace('abc', 'b', 'x')`, so the statement is an expression.
	if startsWithCall(sql) {
		return true
	}
	switch leadingKeyword(sql) {
	case "INSERT", "CREATE", "DROP", "ALTER", "SET", "USE", "TRUNCATE", "RENAME", "OPTIMIZE",
		"SYSTEM", "GRANT", "REVOKE", "ATTACH", "DETACH", "KILL", "EXCHANGE", "UNDROP", "BACKUP", "RESTORE",
		"DELETE", "UPDATE", "REPLACE", "MOVE", "BEGIN", "COMMIT", "ROLLBACK", "START":
		return false
	}
	return true
}

// startsWithCall tells if the first word of sql, after comments and
// whitespace, is directly followed by `(`.
func startsWithCall(sql string) bool {
	i := skipWhitespaceAndComments(sql, 0)
	j := i
	for j < len(sql) && isWordByte(sql[j]) {
		j++
	}
	if j == i {
		return false
	}
	k := skipWhitespaceAndComments(sql, j)
	return k < len(sql) && sql[k] == '('
}

func (r *runner) writeRows(rows driver.Rows, format outputFormat) error {
	types := rows.ColumnTypes()
	renderers := make([]*columnRenderer, len(types))
	for i, t := range types {
		c, err := newColumnRenderer(t.DatabaseTypeName(), t.ScanType())
		if err != nil {
			return err
		}
		renderers[i] = c
	}
	var sb strings.Builder
	// A Raw format writes the header without escapes, as its rows
	// (TabSeparatedRowOutputFormat::writeLine on the server).
	writeHeader := func(get func(i int) string) {
		for i := range types {
			if i > 0 {
				sb.WriteByte('\t')
			}
			if format.Raw {
				sb.WriteString(get(i))
			} else {
				writeString(&sb, get(i), false)
			}
		}
		sb.WriteByte('\n')
	}
	if format.Names {
		writeHeader(func(i int) string { return types[i].Name() })
	}
	if format.Types {
		writeHeader(func(i int) string { return types[i].DatabaseTypeName() })
	}

	dests := make([]any, len(renderers))
	writeRow := func() error {
		if format.Discard {
			// `FORMAT Null`: the values are scanned, but not rendered.
			return nil
		}
		for i, c := range renderers {
			if i > 0 {
				sb.WriteByte('\t')
			}
			if err := c.render(&sb, dests[i], format.Raw); err != nil {
				return fmt.Errorf("column %q: %w", types[i].Name(), err)
			}
		}
		sb.WriteByte('\n')
		return nil
	}
	newDests := func() {
		for i, c := range renderers {
			dests[i] = c.dest()
		}
	}

	for rows.Next() {
		newDests()
		if err := rows.Scan(dests...); err != nil {
			return err
		}
		if err := writeRow(); err != nil {
			return err
		}
		if sb.Len() > 64<<10 {
			r.result.WriteString(sb.String())
			sb.Reset()
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// `WITH TOTALS`: the totals row follows an empty line.
	newDests()
	if err := rows.Totals(dests...); err == nil && !format.Discard {
		sb.WriteByte('\n')
		if err := writeRow(); err != nil {
			return err
		}
	}
	r.result.WriteString(sb.String())
	return nil
}

// formatServerError writes an exception the way `clickhouse-client` does, so
// that `.sh` tests that look for the error name in stderr can find it.
func formatServerError(e *proto.Exception) string {
	msg := e.Message
	if name, ok := errorNames[e.Code]; ok && !strings.Contains(msg, "("+name+")") {
		msg += " (" + name + ")"
	}
	return fmt.Sprintf("Code: %d. %s: %s", e.Code, e.Name, msg)
}

// parseUseDatabase returns the database name of a `USE` statement. A
// `{name:Identifier}` query parameter is replaced by its value.
func parseUseDatabase(sql string, params map[string]string) (string, bool) {
	i := skipWhitespaceAndComments(sql, 0)
	i += len("USE")
	rest := strings.TrimSpace(sql[i:])
	rest = strings.TrimSpace(strings.TrimSuffix(rest, ";"))
	if word, after, ok := strings.Cut(rest, " "); ok && strings.EqualFold(word, "DATABASE") {
		rest = strings.TrimSpace(after)
	}
	switch {
	case rest == "":
		return "", false
	case rest[0] == '`' || rest[0] == '"':
		return unquoteIdentifier(rest)
	case rest[0] == '{':
		name, _, ok := strings.Cut(strings.Trim(rest, "{}"), ":")
		if !ok {
			return "", false
		}
		v, ok := params[strings.TrimSpace(name)]
		return v, ok
	}
	for j := 0; j < len(rest); j++ {
		if !isWordByte(rest[j]) {
			return "", false
		}
	}
	return rest, true
}

func unquoteIdentifier(s string) (string, bool) {
	q := s[0]
	if len(s) < 2 || s[len(s)-1] != q {
		return "", false
	}
	var sb strings.Builder
	for i := 1; i < len(s)-1; i++ {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s)-1:
			i++
			sb.WriteByte(s[i])
		case c == q && i+1 < len(s)-1 && s[i+1] == q:
			i++
			sb.WriteByte(q)
		default:
			sb.WriteByte(c)
		}
	}
	return sb.String(), true
}

// parseSetAssignments returns the settings that a `SET a = 1, b = 'x'`
// statement assigns, with their values (string values without quotes).
func parseSetAssignments(sql string) map[string]string {
	i := skipWhitespaceAndComments(sql, 0) + len("SET")
	// Comments are not part of a name or a value: `SET /* c */ x = 1`.
	body := strings.TrimSuffix(strings.TrimSpace(withoutComments(sql[i:])), ";")
	out := map[string]string{}
	depth, start := 0, 0
	add := func(part string) {
		name, value, ok := strings.Cut(part, "=")
		if !ok {
			return
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '\'' && value[len(value)-1] == '\'' {
			value = value[1 : len(value)-1]
		}
		out[strings.TrimSpace(name)] = value
	}
	for j := 0; j < len(body); j++ {
		switch c := body[j]; c {
		case '\'', '"', '`':
			j = skipQuoted(body, j) - 1
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ',':
			if depth == 0 {
				add(body[start:j])
				start = j + 1
			}
		}
	}
	add(body[start:])
	return out
}

// withoutComments replaces each comment of sql (outside quotes and
// heredocs) with a space.
func withoutComments(sql string) string {
	var sb strings.Builder
	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '\'' || c == '"' || c == '`':
			j := skipQuoted(sql, i)
			sb.WriteString(sql[i:j])
			i = j
		case c == '$' && heredocEnd(sql, i) > 0:
			j := heredocEnd(sql, i)
			sb.WriteString(sql[i:j])
			i = j
		case strings.HasPrefix(sql[i:], "--"),
			strings.HasPrefix(sql[i:], "#!"), strings.HasPrefix(sql[i:], "# "):
			i = skipLineComment(sql, i)
			sb.WriteByte(' ')
		case strings.HasPrefix(sql[i:], "/*"):
			i = skipBlockComment(sql, i)
			sb.WriteByte(' ')
		default:
			sb.WriteByte(c)
			i++
		}
	}
	return sb.String()
}
