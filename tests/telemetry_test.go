package tests

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func telemetryOptions(t *testing.T, protocol clickhouse.Protocol) (*clickhouse.Options, *tracetest.InMemoryExporter, *metric.ManualReader) {
	t.Helper()
	env, err := GetNativeTestEnvironment()
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	var tlsConfig *tls.Config
	port := env.Port
	if protocol == clickhouse.HTTP {
		port = env.HttpPort
	}
	if useSSL {
		tlsConfig = &tls.Config{}
		port = env.SslPort
		if protocol == clickhouse.HTTP {
			port = env.HttpsPort
		}
	}
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(context.Background())) })
	return &clickhouse.Options{
		TLS: tlsConfig, Protocol: protocol, Addr: []string{fmt.Sprintf("%s:%d", env.Host, port)},
		Auth:         clickhouse.Auth{Database: env.Database, Username: env.Username, Password: env.Password},
		MaxOpenConns: 1, BlockBufferSize: 1,
		Telemetry: &clickhouse.TelemetryOptions{TracerProvider: tp, MeterProvider: mp, EnableProfiling: true},
	}, exporter, reader
}

func telemetryDurationCount(t *testing.T, reader *metric.ManualReader) uint64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &data))
	var count uint64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "db.client.operation.duration" {
				for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					count += point.Count
				}
			}
			if m.Name == "clickhouse.client.operations.active" {
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					require.Zero(t, point.Value)
				}
			}
		}
	}
	return count
}

func TestTelemetryNative(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		opt, exporter, reader := telemetryOptions(t, protocol)
		conn, err := clickhouse.Open(opt)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx := context.Background()
		r, err := conn.Query(ctx, "SELECT currentQueryID()")
		require.NoError(t, err)
		require.Empty(t, exporter.GetSpans())
		require.True(t, r.Next())
		var id string
		require.NoError(t, r.Scan(&id))
		require.NoError(t, r.Close())
		require.NoError(t, r.Close())
		spans := exporter.GetSpans()
		require.Len(t, spans, 1)
		require.Contains(t, spans[0].Attributes, attribute.String("clickhouse.query.id", id))
		require.Equal(t, spans[0].SpanContext.TraceID().String()+"-"+spans[0].SpanContext.SpanID().String(), id)

		// Caller IDs survive instrumentation, and QueryRow completes on Scan.
		require.NoError(t, conn.QueryRow(clickhouse.Context(ctx, clickhouse.WithQueryID("telemetry-test-"+id)), "SELECT currentQueryID()").Scan(&id))
		require.Contains(t, id, "telemetry-test-")
		require.Len(t, exporter.GetSpans(), 2)

		var value uint64
		err = conn.QueryRow(ctx, "SELECT 'cannot convert to uint64'").Scan(&value)
		require.Error(t, err)
		require.Len(t, exporter.GetSpans(), 3)
		require.Equal(t, codes.Error, exporter.GetSpans()[2].Status.Code)
		// No result still completes successfully; preserve the public sentinel.
		err = conn.QueryRow(ctx, "SELECT number FROM numbers(0)").Scan(&value)
		require.Equal(t, sql.ErrNoRows, err)
		require.Equal(t, codes.Unset, exporter.GetSpans()[3].Status.Code)

		require.NoError(t, conn.Exec(ctx, "SELECT 1"))
		require.Error(t, conn.Exec(ctx, "SELECT unknown_telemetry_function()"))
		require.Equal(t, codes.Error, exporter.GetSpans()[5].Status.Code)
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, conn.Exec(canceled, "SELECT 1"), context.Canceled)
		require.Equal(t, "canceled", exporter.GetSpans()[6].Status.Description)
		require.EqualValues(t, 7, telemetryDurationCount(t, reader))
	})
}

func TestTelemetrySQL(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		opt, exporter, reader := telemetryOptions(t, protocol)
		db := clickhouse.OpenDB(opt)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		ctx := context.Background()
		r, err := db.QueryContext(ctx, "SELECT currentQueryID()")
		require.NoError(t, err)
		require.Empty(t, exporter.GetSpans())
		require.True(t, r.Next())
		var id string
		require.NoError(t, r.Scan(&id))
		require.NoError(t, r.Close())
		require.NoError(t, r.Close())
		require.Len(t, exporter.GetSpans(), 1)
		require.Contains(t, exporter.GetSpans()[0].Attributes, attribute.String("clickhouse.query.id", id))
		_, err = db.ExecContext(ctx, "SELECT 1")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SELECT unknown_telemetry_function()")
		require.Error(t, err)
		require.Equal(t, codes.Error, exporter.GetSpans()[2].Status.Code)
		require.EqualValues(t, 3, telemetryDurationCount(t, reader))
	})
}

func TestTelemetryBackpressure(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		opt, exporter, reader := telemetryOptions(t, protocol)
		conn, err := clickhouse.Open(opt)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx := context.Background()
		r, err := conn.Query(ctx, "SELECT number FROM numbers(1000000) SETTINGS max_block_size=1000")
		require.NoError(t, err)
		// A full result buffer retains the sole connection; another operation
		// must time out acquiring it. This exercises a real pool and server.
		deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		require.Error(t, conn.Exec(deadline, "SELECT 1"))
		require.Len(t, exporter.GetSpans(), 1)
		require.Equal(t, codes.Error, exporter.GetSpans()[0].Status.Code)
		require.NoError(t, r.Close())
		require.Len(t, exporter.GetSpans(), 2)
		var delivery float64
		for _, attr := range exporter.GetSpans()[1].Attributes {
			if attr.Key == "clickhouse.result_delivery.duration" {
				delivery = attr.Value.AsFloat64()
			}
		}
		require.Positive(t, delivery)
		require.EqualValues(t, 2, telemetryDurationCount(t, reader))
	})
}

func TestTelemetryStreamFailure(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		opt, exporter, reader := telemetryOptions(t, protocol)
		conn, err := clickhouse.Open(opt)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		r, err := conn.Query(context.Background(), "SELECT number, throwIf(number >= 2000) FROM numbers(10000) SETTINGS max_block_size=1000")
		if err == nil {
			for r.Next() {
			}
			err = r.Err()
			closeErr := r.Close()
			if err == nil {
				err = closeErr
			}
		}
		require.Error(t, err)
		require.Len(t, exporter.GetSpans(), 1)
		require.Equal(t, codes.Error, exporter.GetSpans()[0].Status.Code)
		require.EqualValues(t, 1, telemetryDurationCount(t, reader))
	})
}

func TestTelemetryStreamCancellation(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		opt, exporter, reader := telemetryOptions(t, protocol)
		conn, err := clickhouse.Open(opt)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		r, err := conn.Query(ctx, "SELECT number FROM numbers(100000000) SETTINGS max_block_size=1000")
		require.NoError(t, err)
		cancel()
		require.Error(t, r.Close())
		require.Len(t, exporter.GetSpans(), 1)
		require.Equal(t, codes.Error, exporter.GetSpans()[0].Status.Code)
		require.EqualValues(t, 1, telemetryDurationCount(t, reader))
	})
}

func TestTelemetryNativeTracePropagation(t *testing.T) {
	opt, exporter, reader := telemetryOptions(t, clickhouse.Native)
	conn, err := clickhouse.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	if !CheckMinServerServerVersion(conn, 22, 8, 0) {
		t.Skip("requires server trace-context logging")
	}
	var mu sync.Mutex
	var logs []string
	ctx := clickhouse.Context(context.Background(),
		clickhouse.WithSettings(clickhouse.Settings{"send_logs_level": "trace"}),
		clickhouse.WithLogs(func(log *clickhouse.Log) {
			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(log.Text, "OpenTelemetry traceparent") {
				logs = append(logs, log.Text)
			}
		}),
	)
	var value uint8
	require.NoError(t, conn.QueryRow(ctx, "SELECT toUInt8(1)").Scan(&value))
	require.Len(t, exporter.GetSpans(), 1)
	span := exporter.GetSpans()[0].SpanContext
	mu.Lock()
	defer mu.Unlock()
	require.Contains(t, strings.Join(logs, "\n"), "00-"+span.TraceID().String()+"-"+span.SpanID().String()+"-01")
	require.EqualValues(t, 1, telemetryDurationCount(t, reader))
}
