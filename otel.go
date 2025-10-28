package clickhouse

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/ClickHouse/clickhouse-go/v2"
	instrumentationVersion = "2.0.0"
)

// otelTracer returns the tracer for this library.
// It uses the global tracer provider by default.
func otelTracer() trace.Tracer {
	return otel.Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion))
}

// startSpan starts a new span with the given name and options.
// If a span already exists in the context, it will be used as the parent.
func startSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otelTracer().Start(ctx, name, opts...)
}

// spanAttributes returns common attributes for ClickHouse operations.
func spanAttributes(query string, serverAddr string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("db.system", "clickhouse"),
	}

	if serverAddr != "" {
		attrs = append(attrs, attribute.String("db.server.address", serverAddr))
	}

	if query != "" {
		attrs = append(attrs, attribute.String("db.statement", query))
	}

	return attrs
}

// recordError records an error on the span if it's not nil.
func recordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// endSpan ends the span and records the error if present.
func endSpan(span trace.Span, err *error) {
	if err != nil && *err != nil {
		recordError(span, *err)
	}
	span.End()
}

// addServerMetrics adds server-side metrics from ProfileInfo to the span.
func addServerMetrics(span trace.Span, info *ProfileInfo) {
	if info == nil {
		return
	}

	span.SetAttributes(
		attribute.Int64("db.clickhouse.rows", int64(info.Rows)),
		attribute.Int64("db.clickhouse.blocks", int64(info.Blocks)),
		attribute.Int64("db.clickhouse.bytes", int64(info.Bytes)),
		attribute.Bool("db.clickhouse.applied_limit", info.AppliedLimit),
		attribute.Int64("db.clickhouse.rows_before_limit", int64(info.RowsBeforeLimit)),
	)
}

// addProgressMetrics adds progress metrics from Progress to the span.
func addProgressMetrics(span trace.Span, progress *Progress) {
	if progress == nil {
		return
	}

	span.SetAttributes(
		attribute.Int64("db.clickhouse.progress.rows", int64(progress.Rows)),
		attribute.Int64("db.clickhouse.progress.bytes", int64(progress.Bytes)),
		attribute.Int64("db.clickhouse.progress.total_rows", int64(progress.TotalRows)),
		attribute.Int64("db.clickhouse.progress.wrote_rows", int64(progress.WroteRows)),
		attribute.Int64("db.clickhouse.progress.wrote_bytes", int64(progress.WroteBytes)),
	)

	if progress.Elapsed > 0 {
		// Server-side elapsed time
		span.SetAttributes(
			attribute.Int64("db.clickhouse.server.elapsed_ns", int64(progress.Elapsed)),
		)
	}
}

// getServerAddress returns the server address from connection options.
func (ch *clickhouse) getServerAddress() string {
	if len(ch.opts.Addr) > 0 {
		return ch.opts.Addr[0]
	}
	return ""
}

// otelConfig holds OpenTelemetry configuration options.
type otelConfig struct {
	// TracerProvider specifies the tracer provider to use.
	// If nil, the global tracer provider will be used.
	TracerProvider trace.TracerProvider

	// Enabled controls whether tracing is enabled.
	// Default: true if TracerProvider is set or global provider is configured.
	Enabled bool

	// CaptureServerMetrics controls whether to capture server-side metrics
	// from ProfileInfo and Progress callbacks.
	// Default: true
	CaptureServerMetrics bool
}

// OtelOption is a function that configures otelConfig.
type OtelOption func(*otelConfig)

// WithTracerProvider sets a custom tracer provider.
func WithTracerProvider(provider trace.TracerProvider) OtelOption {
	return func(c *otelConfig) {
		c.TracerProvider = provider
		c.Enabled = true
	}
}

// WithOtelEnabled enables or disables OpenTelemetry tracing.
func WithOtelEnabled(enabled bool) OtelOption {
	return func(c *otelConfig) {
		c.Enabled = enabled
	}
}

// WithServerMetrics enables or disables server-side metrics capture.
func WithServerMetrics(capture bool) OtelOption {
	return func(c *otelConfig) {
		c.CaptureServerMetrics = capture
	}
}

// defaultOtelConfig returns the default otelConfig.
func defaultOtelConfig() *otelConfig {
	return &otelConfig{
		Enabled:              false, // Disabled by default to avoid breaking changes
		CaptureServerMetrics: true,
	}
}

// applyOtelOptions applies the given options to the config.
func applyOtelOptions(opts []OtelOption) *otelConfig {
	cfg := defaultOtelConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// isTracingEnabled returns true if tracing is enabled for this connection.
func (ch *clickhouse) isTracingEnabled() bool {
	return ch.otelCfg != nil && ch.otelCfg.Enabled
}

// createQuerySpan creates a span for a query operation with proper attributes.
func (ch *clickhouse) createQuerySpan(ctx context.Context, operation string, query string) (context.Context, trace.Span) {
	if !ch.isTracingEnabled() {
		return ctx, nil
	}

	attrs := spanAttributes(query, ch.getServerAddress())
	attrs = append(attrs,
		attribute.String("db.operation", operation),
		attribute.String("db.clickhouse.protocol", string(ch.opts.Protocol)),
	)

	ctx, span := startSpan(ctx, fmt.Sprintf("clickhouse.%s", operation), trace.WithAttributes(attrs...))

	// If CaptureServerMetrics is enabled, check if user has already set callbacks
	// If not, we'll attach our own. If they have, we'll wrap them.
	if ch.otelCfg.CaptureServerMetrics && span != nil {
		// Check if the context already has callbacks set by the user
		existingOpts := queryOptions(ctx)

		// Only auto-attach if user hasn't set their own callbacks
		if existingOpts.events.profileInfo == nil && existingOpts.events.progress == nil {
			ctx = ch.attachServerMetricsCallbacks(ctx, span)
		}
	}

	return ctx, span
}

// attachServerMetricsCallbacks attaches callbacks to capture server-side metrics.
func (ch *clickhouse) attachServerMetricsCallbacks(ctx context.Context, span trace.Span) context.Context {
	if span == nil {
		return ctx
	}

	// Attach ProfileInfo callback
	ctx = Context(ctx, WithProfileInfo(func(info *ProfileInfo) {
		addServerMetrics(span, info)
	}))

	// Attach Progress callback to capture server elapsed time
	ctx = Context(ctx, WithProgress(func(progress *Progress) {
		addProgressMetrics(span, progress)
	}))

	return ctx
}
