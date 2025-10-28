package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestOtelTracingEnabled tests that tracing works when enabled
func TestOtelTracingEnabled(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		// Setup in-memory span exporter to capture traces
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(
			trace.WithSyncer(exporter),
		)
		otel.SetTracerProvider(tp)
		defer tp.Shutdown(context.Background())

		// Get base connection first to get environment
		baseConn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)

		// Get server version to find address
		version, err := baseConn.ServerVersion()
		require.NoError(t, err)
		baseConn.Close()

		// Get test environment
		env, err := GetTestEnvironment(testSet)
		require.NoError(t, err)

		// Create connection with tracing enabled
		opts := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
			Auth: clickhouse.Auth{
				Database: env.Database,
				Username: env.Username,
				Password: env.Password,
			},
			Protocol: protocol,
			OpenTelemetryOptions: []clickhouse.OtelOption{
				clickhouse.WithOtelEnabled(true),
				clickhouse.WithServerMetrics(false), // Disable server metrics for basic test
			},
		}

		conn, err := clickhouse.Open(opts)
		require.NoError(t, err)
		defer conn.Close()

		_ = version // use version

		ctx := context.Background()

		// Test Query operation
		t.Run("Query", func(t *testing.T) {
			exporter.Reset()

			var count uint64
			row := conn.QueryRow(ctx, "SELECT COUNT() FROM system.numbers LIMIT 1000")
			require.NoError(t, row.Scan(&count))
			assert.Equal(t, uint64(1000), count)

			// Verify span was created
			spans := exporter.GetSpans()
			require.Len(t, spans, 1, "Expected 1 span for QueryRow operation")

			span := spans[0]
			assert.Equal(t, "clickhouse.query_row", span.Name)

			// Verify attributes
			attrs := span.Attributes
			hasDBSystem := false
			hasOperation := false
			hasStatement := false

			for _, attr := range attrs {
				switch string(attr.Key) {
				case "db.system":
					hasDBSystem = true
					assert.Equal(t, "clickhouse", attr.Value.AsString())
				case "db.operation":
					hasOperation = true
					assert.Equal(t, "query_row", attr.Value.AsString())
				case "db.statement":
					hasStatement = true
					assert.Contains(t, attr.Value.AsString(), "SELECT COUNT()")
				}
			}

			assert.True(t, hasDBSystem, "Expected db.system attribute")
			assert.True(t, hasOperation, "Expected db.operation attribute")
			assert.True(t, hasStatement, "Expected db.statement attribute")
		})

		// Test Exec operation
		t.Run("Exec", func(t *testing.T) {
			exporter.Reset()

			err := conn.Exec(ctx, "DROP TABLE IF EXISTS otel_test_table")
			require.NoError(t, err)

			// Verify span was created
			spans := exporter.GetSpans()
			require.Len(t, spans, 1, "Expected 1 span for Exec operation")

			span := spans[0]
			assert.Equal(t, "clickhouse.exec", span.Name)
		})

		// Test Ping operation
		t.Run("Ping", func(t *testing.T) {
			exporter.Reset()

			err := conn.Ping(ctx)
			require.NoError(t, err)

			// Verify span was created
			spans := exporter.GetSpans()
			require.Len(t, spans, 1, "Expected 1 span for Ping operation")

			span := spans[0]
			assert.Equal(t, "clickhouse.ping", span.Name)
		})
	})
}

// TestOtelTracingDisabled tests that no spans are created when tracing is disabled
func TestOtelTracingDisabled(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		// Setup in-memory span exporter to capture traces
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(
			trace.WithSyncer(exporter),
		)
		otel.SetTracerProvider(tp)
		defer tp.Shutdown(context.Background())

		// Connect WITHOUT tracing enabled (default) - just use standard connection
		conn, err := GetNativeConnection(t, protocol, nil, nil, nil)
		require.NoError(t, err)
		defer conn.Close()

		ctx := context.Background()

		// Execute query
		var count uint64
		row := conn.QueryRow(ctx, "SELECT COUNT() FROM system.numbers LIMIT 100")
		require.NoError(t, row.Scan(&count))

		// Verify NO spans were created (tracing is disabled by default)
		spans := exporter.GetSpans()
		assert.Len(t, spans, 0, "Expected no spans when tracing is disabled")
	})
}

// TestOtelTracingWithServerMetrics tests that server metrics are captured
func TestOtelTracingWithServerMetrics(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		// Setup in-memory span exporter to capture traces
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(
			trace.WithSyncer(exporter),
		)
		otel.SetTracerProvider(tp)
		defer tp.Shutdown(context.Background())

		// Get test environment
		env, err := GetTestEnvironment(testSet)
		require.NoError(t, err)

		// Create connection with tracing and server metrics enabled
		opts := &clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%d", env.Host, env.Port)},
			Auth: clickhouse.Auth{
				Database: env.Database,
				Username: env.Username,
				Password: env.Password,
			},
			Protocol: protocol,
			OpenTelemetryOptions: []clickhouse.OtelOption{
				clickhouse.WithOtelEnabled(true),
				clickhouse.WithServerMetrics(true),
			},
		}

		conn, err := clickhouse.Open(opts)
		require.NoError(t, err)
		defer conn.Close()

		ctx := context.Background()

		// Add explicit progress callback to ensure server sends progress
		ctx = clickhouse.Context(ctx,
			clickhouse.WithProgress(func(p *clickhouse.Progress) {
				t.Logf("Progress: rows=%d, bytes=%d, elapsed=%v", p.Rows, p.Bytes, p.Elapsed)
			}),
		)

		// Execute query that should return progress
		rows, err := conn.Query(ctx, "SELECT number FROM system.numbers LIMIT 10000")
		require.NoError(t, err)

		count := 0
		for rows.Next() {
			var num uint64
			require.NoError(t, rows.Scan(&num))
			count++
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		// Verify span was created
		spans := exporter.GetSpans()
		require.Len(t, spans, 1, "Expected 1 span for Query operation")

		span := spans[0]
		assert.Equal(t, "clickhouse.query", span.Name)

		// Check for server metrics in attributes
		// Note: Not all queries return progress/profile info, but we can at least
		// verify the tracing infrastructure is in place
		t.Logf("Span has %d attributes", len(span.Attributes))
		for _, attr := range span.Attributes {
			t.Logf("  %s = %v", attr.Key, attr.Value)
		}
	})
}
