package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/pprof"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestTelemetryPropagation(t *testing.T) {
	state, err := trace.ParseTraceState("vendor=value")
	require.NoError(t, err)
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled, TraceState: state,
	})
	other := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{3}, SpanID: trace.SpanID{4}})
	for _, explicit := range []bool{false, true} {
		ctx := trace.ContextWithSpanContext(context.Background(), parent)
		want := parent
		if explicit {
			ctx = Context(ctx, WithSpan(other))
			want = other
		}
		options := queryOptions(ctx)
		require.Equal(t, want, options.span)
		h := &httpConnect{opt: (&Options{}).setDefaults()}
		req, err := h.createRequest(ctx, "http://localhost:8123", nil, &options, nil)
		require.NoError(t, err)
		got := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.HeaderCarrier(req.Header)))
		require.Equal(t, want.TraceID(), got.TraceID())
		require.Equal(t, want.SpanID(), got.SpanID())
		require.Equal(t, want.TraceFlags(), got.TraceFlags())
		require.Equal(t, want.TraceState(), got.TraceState())
	}
}

func TestTelemetryLifecycle(t *testing.T) {
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(ctx)) })
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, mp.Shutdown(ctx)) })
	tel, err := newTelemetry((&Options{Telemetry: &TelemetryOptions{TracerProvider: tp, MeterProvider: mp}}).setDefaults(), "native")
	require.NoError(t, err)
	ctx, parent := tp.Tracer("test").Start(ctx, "parent")
	defer parent.End()
	ctx, op := tel.start(ctx, "Query", "SELECT 'private-value'")
	require.Equal(t, op.spanContext, queryOptions(ctx).span)
	require.NotEmpty(t, queryOptions(ctx).queryID)
	op.resultReady(ctx)
	require.Empty(t, exporter.GetSpans(), "a result-ready operation must remain open")
	op.finish(context.Canceled)
	op.finish(nil)
	spans := exporter.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, parent.SpanContext().SpanID(), spans[0].Parent.SpanID())
	require.Equal(t, codes.Error, spans[0].Status.Code)
	require.Equal(t, "canceled", spans[0].Status.Description)
	require.NotContains(t, fmt.Sprint(spans[0].Attributes), "private-value")
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &data))
	var durations uint64
	var active int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch m.Name {
			case "db.client.operation.duration":
				for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
					durations += point.Count
					for _, attr := range point.Attributes.ToSlice() {
						require.NotEqual(t, "clickhouse.query.id", string(attr.Key))
						require.NotContains(t, attr.Value.AsString(), "private-value")
					}
				}
			case "clickhouse.client.operations.active":
				for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
					active += point.Value
				}
			}
		}
	}
	require.EqualValues(t, 1, durations)
	require.Zero(t, active)
}

func TestTelemetrySampling(t *testing.T) {
	for _, sampled := range []bool{false, true} {
		name := "unsampled"
		sampler := sdktrace.NeverSample()
		if sampled {
			name = "sampled"
			sampler = sdktrace.AlwaysSample()
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			exporter := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSyncer(exporter))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(ctx)) })
			reader := metric.NewManualReader()
			mp := metric.NewMeterProvider(metric.WithReader(reader))
			t.Cleanup(func() { require.NoError(t, mp.Shutdown(ctx)) })
			tel, err := newTelemetry((&Options{Telemetry: &TelemetryOptions{TracerProvider: tp, MeterProvider: mp}}).setDefaults(), "native")
			require.NoError(t, err)
			ctx, op := tel.start(ctx, "Exec", "SELECT 1")
			require.Equal(t, sampled, queryOptions(ctx).span.IsSampled())
			op.finish(nil)
			var data metricdata.ResourceMetrics
			require.NoError(t, reader.Collect(ctx, &data))
			var count uint64
			for _, scope := range data.ScopeMetrics {
				for _, m := range scope.Metrics {
					if m.Name != "db.client.operation.duration" {
						continue
					}
					for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
						count += point.Count
						if sampled {
							span := exporter.GetSpans()[0]
							require.Equal(t, span.EndTime.Sub(span.StartTime).Seconds(), point.Sum)
						}
					}
				}
			}
			require.EqualValues(t, 1, count)
			if !sampled {
				require.Empty(t, exporter.GetSpans())
			}
		})
	}
}

func TestTelemetryNoopTracerKeepsQueryIDUnset(t *testing.T) {
	tel, err := newTelemetry((&Options{Telemetry: &TelemetryOptions{TracerProvider: noop.NewTracerProvider()}}).setDefaults(), "native")
	require.NoError(t, err)
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	ctx, op := tel.start(trace.ContextWithSpanContext(context.Background(), parent), "Query", "SELECT 1")
	defer op.finish(nil)
	require.Empty(t, queryOptions(ctx).queryID, "a reused parent ID cannot identify concurrent queries")
}

func TestTelemetryProfileScope(t *testing.T) {
	ctx := context.Background()
	tel, err := newTelemetry((&Options{Telemetry: &TelemetryOptions{EnableProfiling: true}}).setDefaults(), "native")
	require.NoError(t, err)
	pprof.Do(ctx, pprof.Labels("application", "test"), func(ctx context.Context) {
		err := observeExec(ctx, tel, "SELECT 1", func(ctx context.Context) error {
			method, ok := pprof.Label(ctx, "clickhouse.method")
			require.True(t, ok)
			require.Equal(t, "Exec", method)
			app, ok := pprof.Label(ctx, "application")
			require.True(t, ok)
			require.Equal(t, "test", app)
			profileReceive(ctx, func() {
				var profile bytes.Buffer
				require.NoError(t, pprof.Lookup("goroutine").WriteTo(&profile, 1))
				require.Contains(t, profile.String(), `"clickhouse.phase":"receive"`)
			})
			return nil
		})
		require.NoError(t, err)
		var profile bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&profile, 1))
		require.NotContains(t, profile.String(), `"clickhouse.method"`)
		require.Contains(t, profile.String(), `"application":"test"`)
	})
}

func TestTelemetryDisabled(t *testing.T) {
	ctx := context.Background()
	want := errors.New("expected")
	err := observeExec(ctx, nil, "SELECT 1", func(got context.Context) error {
		require.Equal(t, ctx, got)
		return want
	})
	require.ErrorIs(t, err, want)
}

type telemetryLogExporter struct {
	mu      sync.Mutex
	records []log.Record
}

func (e *telemetryLogExporter) Export(_ context.Context, records []log.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, record := range records {
		e.records = append(e.records, record.Clone())
	}
	return nil
}

func (e *telemetryLogExporter) Shutdown(context.Context) error   { return nil }
func (e *telemetryLogExporter) ForceFlush(context.Context) error { return nil }

func TestTelemetryOTelLogs(t *testing.T) {
	ctx := context.Background()
	exporter := &telemetryLogExporter{}
	lp := log.NewLoggerProvider(log.WithProcessor(log.NewSimpleProcessor(exporter)))
	t.Cleanup(func() { require.NoError(t, lp.Shutdown(ctx)) })
	spans := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(spans))
	t.Cleanup(func() { require.NoError(t, tp.Shutdown(ctx)) })
	tel, err := newTelemetry((&Options{
		Logger:    otelslog.NewLogger("test", otelslog.WithLoggerProvider(lp)),
		Telemetry: &TelemetryOptions{TracerProvider: tp},
	}).setDefaults(), "native")
	require.NoError(t, err)
	for _, result := range []error{nil, errors.New("private query text")} {
		err := observeExec(ctx, tel, "SELECT 'private value'", func(context.Context) error { return result })
		require.ErrorIs(t, err, result)
	}
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	require.Len(t, exporter.records, 2)
	for i, record := range exporter.records {
		require.Equal(t, spans.GetSpans()[i].SpanContext.TraceID(), record.TraceID())
		require.Equal(t, spans.GetSpans()[i].SpanContext.SpanID(), record.SpanID())
		require.Equal(t, "clickhouse operation completed", record.Body().AsString())
		require.NotContains(t, fmt.Sprint(record), "private")
	}
}

func BenchmarkTelemetryExec(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var tel *telemetry
		if enabled {
			name = "enabled"
			var err error
			tel, err = newTelemetry((&Options{Telemetry: &TelemetryOptions{}}).setDefaults(), "native")
			if err != nil {
				b.Fatal(err)
			}
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := observeExec(context.Background(), tel, "SELECT 1", func(context.Context) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
