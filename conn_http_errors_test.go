package clickhouse

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

func TestParseHTTPException(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		headerCode   string
		headerName   string
		wantNil      bool
		wantCode     int32
		wantName     string
		wantMessage  string
		wantCodeName string
	}{
		{
			name:         "full format with symbol and version",
			text:         "Code: 60. DB::Exception: Unknown table expression identifier 'non_existent_table' in scope SELECT * FROM non_existent_table. (UNKNOWN_TABLE) (version 25.1.5.31 (official build))",
			wantCode:     60,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown table expression identifier 'non_existent_table' in scope SELECT * FROM non_existent_table. (UNKNOWN_TABLE) (version 25.1.5.31 (official build))",
			wantCodeName: "UNKNOWN_TABLE",
		},
		{
			name:         "trailing newline",
			text:         "Code: 81. DB::Exception: Database foo does not exist. (UNKNOWN_DATABASE)\n",
			wantCode:     81,
			wantName:     "DB::Exception",
			wantMessage:  "Database foo does not exist. (UNKNOWN_DATABASE)",
			wantCodeName: "UNKNOWN_DATABASE",
		},
		{
			name:         "multiline message preserved",
			text:         "Code: 62. DB::Exception: Syntax error: failed at position 1\nExpected one of: SELECT, INSERT. (SYNTAX_ERROR)",
			wantCode:     62,
			wantName:     "DB::Exception",
			wantMessage:  "Syntax error: failed at position 1\nExpected one of: SELECT, INSERT. (SYNTAX_ERROR)",
			wantCodeName: "SYNTAX_ERROR",
		},
		{
			name:         "missing version suffix",
			text:         "Code: 241. DB::Exception: Memory limit (total) exceeded. (MEMORY_LIMIT_EXCEEDED)",
			wantCode:     241,
			wantName:     "DB::Exception",
			wantMessage:  "Memory limit (total) exceeded. (MEMORY_LIMIT_EXCEEDED)",
			wantCodeName: "MEMORY_LIMIT_EXCEEDED",
		},
		{
			name:         "net exception class",
			text:         "Code: 210. DB::NetException: Connection refused (localhost:9000). (NETWORK_ERROR)",
			wantCode:     210,
			wantName:     "DB::NetException",
			wantMessage:  "Connection refused (localhost:9000). (NETWORK_ERROR)",
			wantCodeName: "NETWORK_ERROR",
		},
		{
			name:         "no class prefix keeps colon in message",
			text:         "Code: 159. Timeout exceeded: elapsed 5.1 seconds. (TIMEOUT_EXCEEDED)",
			wantCode:     159,
			wantName:     "",
			wantMessage:  "Timeout exceeded: elapsed 5.1 seconds. (TIMEOUT_EXCEEDED)",
			wantCodeName: "TIMEOUT_EXCEEDED",
		},
		{
			name:        "header code only, body not exception-shaped",
			text:        "some proxy mangled body",
			headerCode:  "60",
			wantCode:    60,
			wantName:    "",
			wantMessage: "some proxy mangled body",
		},
		{
			name:         "header code overrides body code",
			text:         "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)",
			headerCode:   "999",
			wantCode:     999,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown table. (UNKNOWN_TABLE)",
			wantCodeName: "UNKNOWN_TABLE",
		},
		{
			name:    "no code anywhere returns nil",
			text:    "DB::Exception: something went wrong",
			wantNil: true,
		},
		{
			name:    "plain html error page returns nil",
			text:    "<html><body>502 Bad Gateway</body></html>",
			wantNil: true,
		},
		{
			name:    "empty body returns nil",
			text:    "",
			wantNil: true,
		},
		{
			name:       "invalid header code ignored",
			text:       "not an exception",
			headerCode: "abc",
			wantNil:    true,
		},
		{
			name:       "zero header code ignored",
			text:       "not an exception",
			headerCode: "0",
			wantNil:    true,
		},
		{
			name:       "negative header code ignored",
			text:       "not an exception",
			headerCode: "-60",
			wantNil:    true,
		},
		{
			name:    "zero body code ignored",
			text:    "Code: 0. DB::Exception: should not become an exception",
			wantNil: true,
		},
		{
			name:       "zero body code rescued by header code",
			text:       "Code: 0. some text",
			headerCode: "60",
			wantCode:   60,
			wantName:   "",
			// The bogus "Code: 0." prefix is not stripped — it is not a code.
			wantMessage: "Code: 0. some text",
		},
		{
			name:         "header name wins over body symbol",
			text:         "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)",
			headerName:   "UNKNOWN_TABLE_FROM_HEADER",
			wantCode:     60,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown table. (UNKNOWN_TABLE)",
			wantCodeName: "UNKNOWN_TABLE_FROM_HEADER",
		},
		{
			name:         "invalid header name falls back to body symbol",
			text:         "Code: 60. DB::Exception: Unknown table. (UNKNOWN_TABLE)",
			headerName:   "not-a-symbol",
			wantCode:     60,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown table. (UNKNOWN_TABLE)",
			wantCodeName: "UNKNOWN_TABLE",
		},
		{
			name:         "multiple all-caps tokens, last wins",
			text:         "Code: 47. DB::Exception: Unknown identifier 'FOO_BAR' in scope. (UNKNOWN_IDENTIFIER)",
			wantCode:     47,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown identifier 'FOO_BAR' in scope. (UNKNOWN_IDENTIFIER)",
			wantCodeName: "UNKNOWN_IDENTIFIER",
		},
		{
			name:         "no symbol in text leaves CodeName empty",
			text:         "Code: 60. DB::Exception: no symbol here",
			wantCode:     60,
			wantName:     "DB::Exception",
			wantMessage:  "no symbol here",
			wantCodeName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ex := parseHTTPException(tt.text, tt.headerCode, tt.headerName)

			if tt.wantNil {
				if ex != nil {
					t.Fatalf("expected nil, got %+v", ex)
				}
				return
			}
			if ex == nil {
				t.Fatal("expected exception, got nil")
			}
			if ex.Code != tt.wantCode {
				t.Errorf("Code: expected %d, got %d", tt.wantCode, ex.Code)
			}
			if ex.Name != tt.wantName {
				t.Errorf("Name: expected %q, got %q", tt.wantName, ex.Name)
			}
			if ex.Message != tt.wantMessage {
				t.Errorf("Message: expected %q, got %q", tt.wantMessage, ex.Message)
			}
			if ex.CodeName != tt.wantCodeName {
				t.Errorf("CodeName: expected %q, got %q", tt.wantCodeName, ex.CodeName)
			}
			if ex.StackTrace != "" || len(ex.Nested) != 0 {
				t.Errorf("StackTrace/Nested should stay empty on HTTP, got %+v", ex)
			}
		})
	}
}

func TestNewHTTPError(t *testing.T) {
	t.Run("exception body unwraps to typed exception", func(t *testing.T) {
		body := []byte("Code: 60. DB::Exception: Unknown table 'foo'. (UNKNOWN_TABLE)")
		err := newHTTPError(404, http.Header{}, body)

		if !strings.HasPrefix(err.Error(), "[HTTP 404] ") {
			t.Errorf("expected [HTTP 404] prefix, got %q", err.Error())
		}
		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatal("expected errors.As to find *Exception")
		}
		if ex.Code != 60 {
			t.Errorf("expected code 60, got %d", ex.Code)
		}
	})

	t.Run("header code wins over body", func(t *testing.T) {
		headers := http.Header{}
		headers.Set(exceptionCodeHeader, "241")
		err := newHTTPError(500, headers, []byte("Code: 60. DB::Exception: mismatch"))

		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatal("expected errors.As to find *Exception")
		}
		if ex.Code != 241 {
			t.Errorf("expected code 241 from header, got %d", ex.Code)
		}
	})

	t.Run("non-exception body falls back to plain error", func(t *testing.T) {
		err := newHTTPError(502, http.Header{}, []byte("<html>Bad Gateway</html>"))

		var ex *Exception
		if errors.As(err, &ex) {
			t.Fatalf("expected no *Exception, got %+v", ex)
		}
		var httpErr *HTTPError
		if !errors.As(err, &httpErr) {
			t.Fatal("expected errors.As to find *HTTPError")
		}
		if httpErr.StatusCode != 502 {
			t.Errorf("expected status 502, got %d", httpErr.StatusCode)
		}
		if !strings.Contains(err.Error(), "Bad Gateway") {
			t.Errorf("expected raw body preserved in error string, got %q", err.Error())
		}
	})

	t.Run("mixed binary and exception block body", func(t *testing.T) {
		// A failed streaming response with buffered/compressed output: the
		// non-200 body carries partial Native-format data followed by an
		// __exception__ block. Message must be the exception text, not the
		// raw block bytes.
		body := []byte("\x01\x00\x02\xff\xff\xff\xff\x00\x01\x01\"throwIf\x05UInt8\x00\r\n__exception__\r\n1234567890123456\r\nCode: 395. DB::Exception: boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n42 1234567890123456\r\n__exception__\r\n")
		headers := http.Header{}
		headers.Set(exceptionCodeHeader, "395")
		err := newHTTPError(500, headers, body)

		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatal("expected errors.As to find *Exception")
		}
		if ex.Code != 395 || ex.CodeName != "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" {
			t.Errorf("unexpected exception: %+v", ex)
		}
		if ex.Message != "boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)" {
			t.Errorf("Message should be the exception text, got %q", ex.Message)
		}
	})

	t.Run("binary body with bare appended exception text", func(t *testing.T) {
		// Same scenario without the __exception__ marker (observed on a
		// buffered gzip 500): the exception text is appended directly after
		// the partial data. The header-vouched code locates it.
		body := []byte("\x01\x00\x02\xff\xff\xff\xff\x00\x01\x01\"throwIf\x05UInt8\x00Code: 395. DB::Exception: boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n")
		headers := http.Header{}
		headers.Set(exceptionCodeHeader, "395")
		err := newHTTPError(500, headers, body)

		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatal("expected errors.As to find *Exception")
		}
		if ex.Code != 395 || ex.CodeName != "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" {
			t.Errorf("unexpected exception: %+v", ex)
		}
		if ex.Message != "boom. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)" {
			t.Errorf("Message should be the exception text, got %q", ex.Message)
		}
	})

	t.Run("errors.As traverses caller %w wrapping", func(t *testing.T) {
		inner := newHTTPError(500, http.Header{}, []byte("Code: 373. DB::Exception: Session is locked. (SESSION_IS_LOCKED)"))
		err := fmt.Errorf("batch sendStreamQuery: %w", inner)

		var httpErr *HTTPError
		var ex *Exception
		if !errors.As(err, &httpErr) || !errors.As(err, &ex) {
			t.Fatal("expected both *HTTPError and *Exception through %w chain")
		}
		if !strings.Contains(err.Error(), "SESSION_IS_LOCKED") {
			t.Errorf("expected symbol preserved in error string, got %q", err.Error())
		}
	})
}

func TestMidStreamException(t *testing.T) {
	t.Run("exception text yields bare typed exception", func(t *testing.T) {
		err := midStreamException("Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)")

		var ex *Exception
		if !errors.As(err, &ex) {
			t.Fatal("expected errors.As to find *Exception")
		}
		if ex.Code != 395 || ex.Name != "DB::Exception" || ex.CodeName != "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO" {
			t.Errorf("unexpected exception: %+v", ex)
		}
		var httpErr *HTTPError
		if errors.As(err, &httpErr) {
			t.Error("mid-stream exception must not be wrapped in *HTTPError")
		}
	})

	t.Run("code-less text keeps legacy fallback", func(t *testing.T) {
		err := midStreamException("something unstructured")

		var ex *Exception
		if errors.As(err, &ex) {
			t.Fatalf("expected no *Exception, got %+v", ex)
		}
		if !strings.Contains(err.Error(), "ClickHouse exception: something unstructured") {
			t.Errorf("expected legacy fallback message, got %q", err.Error())
		}
	})
}
