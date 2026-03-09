package clickhouse

import (
	"bytes"
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
