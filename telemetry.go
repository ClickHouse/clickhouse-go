package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// TelemetryOptions configures optional client telemetry.
type TelemetryOptions = driver.TelemetryOptions

const telemetryScope = "github.com/ClickHouse/clickhouse-go/v2"

type telemetryOperationKey struct{}

type telemetry struct {
	delivery  metric.Float64Histogram
	tracer    trace.Tracer
	duration  metric.Float64Histogram
	ready     metric.Float64Histogram
	acquire   metric.Float64Histogram
	active    metric.Int64UpDownCounter
	logger    *slog.Logger
	attrs     []attribute.KeyValue
	profiling bool
}

func newTelemetry(opt *Options, api string) (*telemetry, error) {
	if opt.Telemetry == nil {
		return nil, nil
	}
	t := &telemetry{
		logger: opt.logger(), profiling: opt.Telemetry.EnableProfiling,
		attrs: []attribute.KeyValue{
			attribute.String("db.system.name", "clickhouse"),
			attribute.String("db.namespace", opt.Auth.Database),
			attribute.String("clickhouse.client.api", api),
			attribute.String("clickhouse.client.protocol", strings.ToLower(opt.Protocol.String())),
		},
	}
	if p := opt.Telemetry.TracerProvider; p != nil {
		t.tracer = p.Tracer(telemetryScope)
	}
	if p := opt.Telemetry.MeterProvider; p != nil {
		m := p.Meter(telemetryScope)
		var err error
		t.duration, err = m.Float64Histogram("db.client.operation.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10), metric.WithDescription("Time from operation start to completion. This includes the time the application uses to read results."))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: create operation duration metric: %w", err)
		}
		t.ready, err = m.Float64Histogram("clickhouse.client.query.result_ready.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10), metric.WithDescription("Time until Query returns a result. The first block can contain only column metadata."))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: create result ready metric: %w", err)
		}
		t.acquire, err = m.Float64Histogram("clickhouse.client.connection.acquire.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10), metric.WithDescription("Time to get a connection through the native API. This includes pool waits, connection checks, and connection setup."))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: create acquisition metric: %w", err)
		}
		t.delivery, err = m.Float64Histogram("clickhouse.client.query.result_delivery.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10), metric.WithDescription("Total time to send decoded blocks to the result buffer. This includes waits for the application to read results."))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: create result delivery metric: %w", err)
		}
		t.active, err = m.Int64UpDownCounter("clickhouse.client.operations.active", metric.WithUnit("{operation}"), metric.WithDescription("Number of operations that started but did not finish. This includes queries with open results."))
		if err != nil {
			return nil, fmt.Errorf("clickhouse: create active operations metric: %w", err)
		}
	}
	return t, nil
}

type telemetryOperation struct {
	deliveryNanos atomic.Int64
	telemetry     *telemetry
	span          trace.Span
	spanContext   trace.SpanContext
	start         time.Time
	attrs         []attribute.KeyValue
	method        string
	queryID       string
	once          sync.Once
	// Only the goroutine that reads results accesses this field.
	err error
}

func (t *telemetry) start(ctx context.Context, method, query string) (context.Context, *telemetryOperation) {
	if t == nil {
		return ctx, nil
	}
	o := &telemetryOperation{telemetry: t, start: time.Now(), method: method}
	o.attrs = append(append([]attribute.KeyValue(nil), t.attrs...), attribute.String("clickhouse.client.method", method))
	name := "clickhouse." + method
	// Use only recognized SQL verbs as metric labels.
	// Do not use other SQL text as metric labels.
	verb, _, _ := strings.Cut(strings.TrimSpace(query), " ")
	if i := strings.IndexAny(verb, "\t\r\n"); i >= 0 {
		verb = verb[:i]
	}
	switch strings.ToUpper(verb) {
	case "SELECT", "INSERT", "ALTER", "CREATE", "DROP", "TRUNCATE", "DELETE", "UPDATE", "EXPLAIN", "SHOW", "DESCRIBE", "EXISTS", "SET", "SYSTEM", "OPTIMIZE":
		name = strings.ToUpper(verb)
		o.attrs = append(o.attrs, attribute.String("db.operation.name", name))
	}
	options := queryOptions(ctx)
	// Use the context from WithSpan as the parent if the caller supplied it.
	// The new client span and the server then use the same trace.
	if options.span.IsValid() {
		ctx = trace.ContextWithSpanContext(ctx, options.span)
	}
	parent := trace.SpanContextFromContext(ctx)
	if t.tracer != nil {
		ctx, o.span = t.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient), trace.WithTimestamp(o.start), trace.WithAttributes(o.attrs...))
	}
	o.spanContext = trace.SpanContextFromContext(ctx)
	o.queryID = options.queryID
	if o.queryID == "" && o.spanContext.IsValid() && o.spanContext.SpanID() != parent.SpanID() {
		o.queryID = o.spanContext.TraceID().String() + "-" + o.spanContext.SpanID().String()
		ctx = Context(ctx, WithQueryID(o.queryID))
	}
	if o.spanContext.IsValid() {
		ctx = Context(ctx, WithSpan(o.spanContext))
	}
	if o.span != nil && o.queryID != "" {
		o.span.SetAttributes(attribute.String("clickhouse.query.id", o.queryID))
	}
	if t.active != nil {
		t.active.Add(ctx, 1, metric.WithAttributes(o.attrs...))
	}
	return context.WithValue(ctx, telemetryOperationKey{}, o), o
}

func (o *telemetryOperation) run(ctx context.Context, fn func(context.Context)) {
	if o == nil || !o.telemetry.profiling {
		fn(ctx)
		return
	}
	pprof.Do(ctx, pprof.Labels("clickhouse.method", o.method, "clickhouse.phase", "submit"), fn)
}

func (o *telemetryOperation) resultReady(ctx context.Context) {
	if o == nil {
		return
	}
	if o.span != nil {
		o.span.AddEvent("clickhouse.result_ready")
	}
	if o.telemetry.ready != nil {
		o.telemetry.ready.Record(ctx, time.Since(o.start).Seconds(), metric.WithAttributes(o.attrs...))
	}
}

func telemetryErrorType(err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if errors.Is(err, ErrAcquireConnTimeout) {
		return "acquire_timeout"
	}
	var exception *Exception
	if errors.As(err, &exception) {
		return fmt.Sprintf("clickhouse.%d", exception.Code)
	}
	return "client_error"
}

func (o *telemetryOperation) finish(err error) {
	if o == nil {
		return
	}
	o.once.Do(func() {
		if err == nil {
			err = o.err
		}
		// Use the span context to connect metrics and logs to the trace.
		// Do not store the caller's context or its values in rows.
		ctx := trace.ContextWithSpanContext(context.Background(), o.spanContext)
		ended := time.Now()
		elapsed := ended.Sub(o.start).Seconds()
		attrs := o.attrs
		level := slog.LevelDebug
		logAttrs := []slog.Attr{slog.String("method", o.method), slog.String("query_id", o.queryID), slog.Float64("duration_seconds", elapsed)}
		if o.spanContext.IsValid() {
			logAttrs = append(logAttrs, slog.String("trace_id", o.spanContext.TraceID().String()), slog.String("span_id", o.spanContext.SpanID().String()))
		}
		if err != nil {
			kind := telemetryErrorType(err)
			attrs = append(append([]attribute.KeyValue(nil), attrs...), attribute.String("error.type", kind))
			level = slog.LevelError
			logAttrs = append(logAttrs, slog.String("error.type", kind))
			// Error messages can contain SQL text and parameter values.
			// Export only the error class.
			if o.span != nil {
				o.span.SetAttributes(attribute.String("error.type", kind))
				o.span.SetStatus(codes.Error, kind)
			}
		}
		if o.method != "Exec" {
			delivery := float64(o.deliveryNanos.Load()) / float64(time.Second)
			if o.span != nil {
				o.span.SetAttributes(attribute.Float64("clickhouse.result_delivery.duration", delivery))
			}
			if o.telemetry.delivery != nil {
				o.telemetry.delivery.Record(ctx, delivery, metric.WithAttributes(o.attrs...))
			}
		}
		if o.telemetry.duration != nil {
			o.telemetry.duration.Record(ctx, elapsed, metric.WithAttributes(attrs...))
		}
		if o.telemetry.active != nil {
			o.telemetry.active.Add(ctx, -1, metric.WithAttributes(o.attrs...))
		}
		o.telemetry.logger.LogAttrs(ctx, level, "clickhouse operation completed", logAttrs...)
		if o.span != nil {
			o.span.End(trace.WithTimestamp(ended))
		}
	})
}

func observeQuery(ctx context.Context, t *telemetry, method, query string, fn func(context.Context) (*rows, error)) (*rows, error) {
	if t == nil {
		return fn(ctx)
	}
	ctx, op := t.start(ctx, method, query)
	var r *rows
	var err error
	op.run(ctx, func(ctx context.Context) { r, err = fn(ctx) })
	if err != nil {
		op.finish(err)
		return nil, err
	}
	op.resultReady(ctx)
	r.operation = op
	return r, nil
}

func observeExec(ctx context.Context, t *telemetry, query string, fn func(context.Context) error) error {
	if t == nil {
		return fn(ctx)
	}
	ctx, op := t.start(ctx, "Exec", query)
	var err error
	op.run(ctx, func(ctx context.Context) { err = fn(ctx) })
	op.finish(err)
	return err
}

// profileReceive uses an existing query receiver goroutine.
// The goroutine exits when the stream completes, fails, or is canceled.
// This function does not create a goroutine.
func profileReceive(ctx context.Context, fn func()) {
	op, ok := ctx.Value(telemetryOperationKey{}).(*telemetryOperation)
	if !ok || !op.telemetry.profiling {
		fn()
		return
	}
	pprof.Do(ctx, pprof.Labels("clickhouse.phase", "receive"), func(context.Context) { fn() })
}

func deliverResult(ctx context.Context, stream chan<- *proto.Block, block *proto.Block) {
	op, ok := ctx.Value(telemetryOperationKey{}).(*telemetryOperation)
	if !ok {
		stream <- block
		return
	}
	start := time.Now()
	stream <- block
	op.deliveryNanos.Add(int64(time.Since(start)))
}
