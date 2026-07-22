package clickhouse

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
)

// exceptionCodeHeader carries the server exception code on non-200 HTTP
// responses. It is unavailable for exceptions that occur mid-stream on a 200
// response, because headers have already been sent by then.
const exceptionCodeHeader = "X-ClickHouse-Exception-Code"

// exceptionNameHeader would carry the symbolic error name (e.g.
// "UNKNOWN_TABLE"). No server sends it yet; when one does, it takes
// precedence over recovering the name from the error text.
const exceptionNameHeader = "X-ClickHouse-Exception-Name"

// maxErrorBodySize caps how much of an error response body is read. Server
// exception text fits well within it (the exception block is at most 16KiB);
// anything larger is a misbehaving server or proxy and gets truncated.
const maxErrorBodySize = 64 * 1024

// HTTPError is returned for requests over the HTTP protocol that fail with a
// non-200 status code. Err is a *Exception when the response body parsed as a
// ClickHouse server exception, otherwise a plain error carrying the raw body.
//
// Use errors.As to branch on either layer:
//
//	var httpErr *clickhouse.HTTPError
//	var chErr   *clickhouse.Exception
//	if errors.As(err, &chErr)   { /* chErr.Code — works on native and HTTP */ }
//	if errors.As(err, &httpErr) { /* httpErr.StatusCode — HTTP only */ }
//
// Note that a query can also fail after the server has started streaming a 200
// response; the status code was flushed before the query failed, so it carries
// no signal. Such mid-stream failures surface as a bare *Exception, not an
// HTTPError — StatusCode is therefore always the status of a genuine non-200
// response.
type HTTPError struct {
	StatusCode int
	Err        error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("[HTTP %d] %v", e.StatusCode, e.Err)
}

func (e *HTTPError) Unwrap() error { return e.Err }

// httpExceptionCodeRe matches the "Code: NNN." prefix the server puts on
// exception text, e.g. "Code: 60. DB::Exception: Unknown table ...".
var httpExceptionCodeRe = regexp.MustCompile(`^Code:\s*(\d+)\.\s*`)

// exceptionMarker frames server exception text embedded in a response body.
const exceptionMarker = "__exception__"

// exceptionTextStartRe locates the start of exception text inside an
// "__exception__" block; same prefix as httpExceptionCodeRe but unanchored.
var exceptionTextStartRe = regexp.MustCompile(`Code:\s*(\d+)\.`)

// exceptionTrailerRe matches the "<message_length> <tag>" trailer line that
// newer servers write between the exception text and the closing
// "__exception__" marker.
var exceptionTrailerRe = regexp.MustCompile(`^\d+ \S+$`)

// exceptionCodeNameRe matches the symbolic error name the server appends to
// exception text, e.g. "(UNKNOWN_TABLE)". The last match wins; the trailing
// "(version ...)" suffix is not all-caps and never matches.
var exceptionCodeNameRe = regexp.MustCompile(`\(([A-Z][A-Z0-9_]+)\)`)

// exceptionCodeNameValueRe validates a symbolic error name on its own, e.g.
// an exceptionNameHeader value.
var exceptionCodeNameValueRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]+$`)

// newHTTPError builds the error for a non-200 HTTP response, best-effort
// parsing the body as a ClickHouse exception. It never fails: when the body
// does not look like a server exception, it falls back to a plain error
// preserving the raw body text.
func newHTTPError(statusCode int, headers http.Header, body []byte) *HTTPError {
	// A failed streaming response (e.g. with buffered or compressed output)
	// can carry partial result data before the exception — framed in an
	// "__exception__" block, or appended as bare text. Extract the exception
	// text so Message is not raw block bytes. In the bare case the anchor is
	// the code the header vouches for; the exception sits at the end of the
	// body, hence the last occurrence.
	text := string(body)
	if msg, ok := exceptionTextFromBlock(text); ok {
		text = msg
	} else if headerCode := headers.Get(exceptionCodeHeader); headerCode != "" {
		if idx := strings.LastIndex(text, "Code: "+headerCode+"."); idx >= 0 {
			text = text[idx:]
		}
	}
	if ex := parseHTTPException(text, headers.Get(exceptionCodeHeader), headers.Get(exceptionNameHeader)); ex != nil {
		return &HTTPError{StatusCode: statusCode, Err: ex}
	}
	return &HTTPError{StatusCode: statusCode, Err: fmt.Errorf("response body: %q", string(body))}
}

// exceptionTextFromBlock extracts server exception text from a body that
// contains an "__exception__" block. The framing around the text varies
// across server versions (newer servers put a 16-byte tag before the message
// and a "<length> <tag>" trailer plus closing marker after it; older servers,
// e.g. 25.8, write the bare message straight after the marker), so the text
// is located by its stable "Code: NNN." prefix instead of by offsets.
func exceptionTextFromBlock(dataStr string) (string, bool) {
	firstMarker := strings.Index(dataStr, exceptionMarker)
	if firstMarker < 0 {
		return "", false
	}
	region := dataStr[firstMarker+len(exceptionMarker):]
	ms := exceptionTextStartRe.FindAllStringSubmatchIndex(region, -1)
	if ms == nil {
		return "", false
	}

	// Anchor on the first "Code: NNN.". Exception text can embed the same
	// pattern (user strings are echoed into error messages), so a later match
	// must not move the anchor — unless it restates the same code, which
	// means the block contains several dumps of one message and the last,
	// complete one wins.
	anchor := ms[0]
	firstCode := region[anchor[2]:anchor[3]]
	for _, m := range ms[1:] {
		if region[m[2]:m[3]] == firstCode {
			anchor = m
		}
	}

	msg := region[anchor[0]:]
	framed := false
	if end := strings.Index(msg, exceptionMarker); end >= 0 {
		msg, framed = msg[:end], true
	}
	msg = strings.TrimRight(msg, "\r\n")
	if framed {
		// Drop the "<message_length> <tag>" trailer line preceding the
		// closing marker.
		if lines := strings.Split(msg, "\n"); len(lines) > 1 && exceptionTrailerRe.MatchString(lines[len(lines)-1]) {
			msg = strings.Join(lines[:len(lines)-1], "\n")
		}
	}
	return msg, true
}

// parseHTTPException parses server exception text of the form
//
//	Code: 60. DB::Exception: Unknown table expression identifier 'x' in scope ... (UNKNOWN_TABLE) (version 25.1.5.31 (official build))
//
// into an Exception, mirroring the native protocol semantics: Name is the
// exception class ("DB::Exception", "DB::NetException", ...) and Message is
// the text that follows it. CodeName is the symbolic error name
// ("UNKNOWN_TABLE"), recovered best-effort from the text. StackTrace and
// Nested stay empty — the HTTP transport does not provide them. headerCode
// and headerName (the X-ClickHouse-Exception-* values, may be empty) take
// precedence over what is found in the text.
//
// Returns nil when the text does not look like a server exception, i.e. no
// error code is available from either source.
func parseHTTPException(text, headerCode, headerName string) *Exception {
	msg := strings.TrimSpace(text)

	// Error codes are strictly positive; 0 means OK and anything non-positive
	// is a mangled body or header, not a server exception.
	var code int64
	var haveCode bool
	if m := httpExceptionCodeRe.FindStringSubmatch(msg); m != nil {
		if c, err := strconv.ParseInt(m[1], 10, 32); err == nil && c > 0 {
			code = c
			haveCode = true
			msg = msg[len(m[0]):]
		}
	}
	if headerCode != "" {
		if c, err := strconv.ParseInt(headerCode, 10, 32); err == nil && c > 0 {
			code = c
			haveCode = true
		}
	}
	if !haveCode {
		return nil
	}

	// The exception class precedes the first ": ", e.g. "DB::Exception: ..."
	// (plain ":" would split inside the "DB::" qualifier). Require a
	// whitespace-free token containing "exception" so a colon inside a plain
	// message is not misread as a class name.
	var name string
	if idx := strings.Index(msg, ": "); idx > 0 {
		cand := msg[:idx]
		if !strings.ContainsAny(cand, " \t\r\n") && strings.Contains(strings.ToLower(cand), "exception") {
			name = cand
			msg = strings.TrimSpace(msg[idx+1:])
		}
	}

	// The symbolic error name, e.g. "(UNKNOWN_TABLE)". Best-effort: a message
	// whose text happens to end with a parenthesized ALL_CAPS token can
	// mislabel, which is why callers should branch on Code, not CodeName.
	var codeName string
	if exceptionCodeNameValueRe.MatchString(headerName) {
		codeName = headerName
	} else if ms := exceptionCodeNameRe.FindAllStringSubmatch(msg, -1); ms != nil {
		codeName = ms[len(ms)-1][1]
	}

	return &Exception{Code: int32(code), Name: name, CodeName: codeName, Message: msg}
}

// midStreamException converts exception text extracted from a mid-stream
// "__exception__" block into a typed *Exception, falling back to a plain
// error when the text does not parse as a server exception.
func midStreamException(errorMsg string) error {
	if ex := parseHTTPException(errorMsg, "", ""); ex != nil {
		return ex
	}
	return fmt.Errorf("ClickHouse exception: %s", errorMsg)
}
