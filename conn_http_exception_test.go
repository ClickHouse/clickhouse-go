package clickhouse

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestParseExceptionFromBytes(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid exception with complete format",
			data:        []byte("\r\n__exception__\r\n1234567890123456\r\nDB::Exception: Table default.test_table doesn't exist\n42 1234567890123456\r\n__exception__\r\n"),
			expectError: true,
			errorMsg:    "DB::Exception: Table default.test_table doesn't exist",
		},
		{
			name:        "exception with multiline error message",
			data:        []byte("\r\n__exception__\r\n1234567890123456\r\nDB::Exception: Syntax error\nExpected identifier\n50 1234567890123456\r\n__exception__\r\n"),
			expectError: true,
			errorMsg:    "DB::Exception: Syntax error\nExpected identifier",
		},
		{
			name:        "exception without second marker",
			data:        []byte("\r\n__exception__\r\n1234567890123456\r\nDB::Exception: Connection timeout"),
			expectError: true,
			errorMsg:    "DB::Exception: Connection timeout",
		},
		{
			name:        "no exception marker",
			data:        []byte("some random data without exception marker"),
			expectError: true,
			errorMsg:    "exception marker not found",
		},
		{
			name:        "exception marker only",
			data:        []byte("__exception__\r\n\r\n\r\n__exception__"),
			expectError: true,
			errorMsg:    "ClickHouse exception occurred but message is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseExceptionFromBytes(tt.data)

			if !tt.expectError {
				if err != nil {
					t.Errorf("expected no error, got: %v", err)
				}
				return
			}

			if err == nil {
				t.Error("expected error, got nil")
				return
			}

			if !strings.Contains(err.Error(), tt.errorMsg) {
				t.Errorf("expected error to contain '%s', got: %v", tt.errorMsg, err)
			}
		})
	}
}

// Real servers prefix the exception payload with "Code: NNN.", which the
// parser turns into a typed *Exception. The code-less fixtures above stay on
// the legacy plain-error fallback.
func TestParseExceptionFromBytesTyped(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		wantCode     int32
		wantName     string
		wantMessage  string
		wantCodeName string
	}{
		{
			name:         "typed exception with complete format",
			data:         []byte("\r\n__exception__\r\n1234567890123456\r\nCode: 395. DB::Exception: Value passed to 'throwIf' function is non-zero: there is an exception. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.1.5.31 (official build))\n42 1234567890123456\r\n__exception__\r\n"),
			wantCode:     395,
			wantName:     "DB::Exception",
			wantMessage:  "Value passed to 'throwIf' function is non-zero: there is an exception. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.1.5.31 (official build))",
			wantCodeName: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
		},
		{
			name:         "typed exception without second marker",
			data:         []byte("\r\n__exception__\r\n1234567890123456\r\nCode: 60. DB::Exception: Unknown table expression identifier 'foo'. (UNKNOWN_TABLE)"),
			wantCode:     60,
			wantName:     "DB::Exception",
			wantMessage:  "Unknown table expression identifier 'foo'. (UNKNOWN_TABLE)",
			wantCodeName: "UNKNOWN_TABLE",
		},
		{
			// Older servers (e.g. 25.8): bare message after the marker — no
			// tag, no trailer line, no closing marker.
			name:         "unframed exception (25.8 layout)",
			data:         []byte("UInt8\x00__exception__\r\nCode: 395. DB::Exception: boom: while executing 'FUNCTION throwIf'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.8.28.1 (official build))\n"),
			wantCode:     395,
			wantName:     "DB::Exception",
			wantMessage:  "boom: while executing 'FUNCTION throwIf'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO) (version 25.8.28.1 (official build))",
			wantCodeName: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
		},
		{
			// The block may contain more than one dump of the message (e.g. a
			// truncated one consumed by block decode, then a complete one);
			// the last dump wins.
			name:         "double dump takes the last complete message",
			data:         []byte("__exception__\r\nCode: 395. DB::Exception: truncat\nexception__\nCode: 395. DB::Exception: complete message. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)\n"),
			wantCode:     395,
			wantName:     "DB::Exception",
			wantMessage:  "complete message. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)",
			wantCodeName: "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := parseExceptionFromBytes(tt.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}

			var ex *Exception
			if !errors.As(err, &ex) {
				t.Fatalf("expected errors.As to find *Exception, got: %v", err)
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

			var httpErr *HTTPError
			if errors.As(err, &httpErr) {
				t.Error("mid-stream exception must not be wrapped in *HTTPError")
			}
		})
	}
}

func TestCapturingReader(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		readSize int
	}{
		{
			name:     "capture small data",
			data:     "test data",
			readSize: 4,
		},
		{
			name:     "capture large data",
			data:     strings.Repeat("x", 1000),
			readSize: 100,
		},
		{
			name:     "capture empty data",
			data:     "",
			readSize: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.NewBufferString(tt.data)
			cr := &capturingReader{reader: buf}

			// Read data in chunks
			chunk := make([]byte, tt.readSize)
			totalRead := 0
			for {
				n, err := cr.Read(chunk)
				totalRead += n
				if err != nil {
					break
				}
			}

			// Verify that all data was captured
			captured := cr.buffer.String()
			if captured != tt.data {
				t.Errorf("expected captured data to be %q, got %q", tt.data, captured)
			}

			if totalRead != len(tt.data) {
				t.Errorf("expected to read %d bytes, got %d", len(tt.data), totalRead)
			}
		})
	}
}
