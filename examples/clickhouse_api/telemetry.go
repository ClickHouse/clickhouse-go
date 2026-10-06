package clickhouse_api

import (
	"context"
	"errors"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// InstrumentedQuery uses application-owned providers. Configure their exporters
// and sampling before calling this function, and shut them down at application
// exit. Collect CPU/goroutine profiles with your application's pprof endpoint or
// profiler; the driver supplies method and phase labels, not a profile exporter.
func InstrumentedQuery(ctx context.Context, options clickhouse.Options, traces trace.TracerProvider, metrics metric.MeterProvider, logs log.LoggerProvider) (err error) {
	options.Telemetry = &clickhouse.TelemetryOptions{
		TracerProvider:  traces,
		MeterProvider:   metrics,
		EnableProfiling: true,
	}
	options.Logger = otelslog.NewLogger("clickhouse-client", otelslog.WithLoggerProvider(logs))
	conn, err := clickhouse.Open(&options)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	ctx, span := traces.Tracer("application").Start(ctx, "load data")
	defer span.End()
	var count uint64
	return conn.QueryRow(ctx, "SELECT count() FROM numbers(1000)").Scan(&count)
}
