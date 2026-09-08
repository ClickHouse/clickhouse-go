package clickhouse

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestLegacyDebugfBackwardCompatibility tests that the old Debug/Debugf options still work
func TestLegacyDebugfBackwardCompatibility(t *testing.T) {
	var buf bytes.Buffer
	called := false

	opt := &Options{
		Debug: true,
		Debugf: func(format string, v ...any) {
			called = true
			buf.WriteString("LEGACY: ")
			buf.WriteString(format)
		},
	}

	logger := opt.logger()
	logger.Debug("test message")

	if !called {
		t.Error("Legacy Debugf was not called")
	}

	if !strings.Contains(buf.String(), "LEGACY:") {
		t.Error("Legacy Debugf did not write expected prefix")
	}
}

// TestNewLoggerOption tests the new Logger option with slog
func TestNewLoggerOption(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	customLogger := slog.New(handler)

	opt := &Options{
		Logger: customLogger,
	}

	logger := opt.logger()
	logger.Debug("test message", slog.String("key", "value"))

	output := buf.String()
	if !strings.Contains(output, "test message") {
		t.Error("Expected message not found in log output")
	}
	if !strings.Contains(output, "key=value") {
		t.Error("Expected structured field not found in log output")
	}
}

// TestNoopLoggerDefault tests that noop logger is used when no logging is configured
func TestNoopLoggerDefault(t *testing.T) {
	opt := &Options{}
	logger := opt.logger()

	// Should not panic with noop logger
	logger.Debug("test message")
	logger.Info("test message")
	logger.Warn("test message")
	logger.Error("test message")
}

// TestLegacyDebugfPriority tests that legacy Debugf takes priority over new Logger
func TestLegacyDebugfPriority(t *testing.T) {
	var buf bytes.Buffer
	legacyCalled := false

	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	customLogger := slog.New(handler)

	opt := &Options{
		Debug: true,
		Debugf: func(format string, v ...any) {
			legacyCalled = true
		},
		Logger: customLogger,
	}

	logger := opt.logger()
	logger.Debug("test message")

	// Legacy Debugf should take priority
	if !legacyCalled {
		t.Error("Legacy Debugf should take priority but was not called")
	}

	// Buffer should be empty since legacy Debugf was used instead
	if buf.Len() > 0 {
		t.Error("New Logger should not be used when legacy Debugf is set")
	}
}

// TestPrepareConnLogger tests the connection logger enrichment
func TestPrepareConnLogger(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	baseLogger := slog.New(handler)

	connLogger := prepareConnLogger(baseLogger, 123, "localhost:9000", "native")
	connLogger.Debug("connection established")

	output := buf.String()
	if !strings.Contains(output, "conn_id=123") {
		t.Error("Expected conn_id in log output")
	}
	if !strings.Contains(output, "remote_addr=localhost:9000") {
		t.Error("Expected remote_addr in log output")
	}
	if !strings.Contains(output, "protocol=native") {
		t.Error("Expected protocol in log output")
	}
}
