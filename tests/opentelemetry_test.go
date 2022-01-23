package tests

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"
)

func TestOpenTelemetry(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if assert.NoError(t, err) {
		var count uint64
		rows := conn.QueryRow(clickhouse.Context(context.Background(), clickhouse.WithSpan(
			trace.NewSpanContext(trace.SpanContextConfig{
				SpanID:  trace.SpanID{1, 2, 3, 4, 5},
				TraceID: trace.TraceID{5, 4, 3, 2, 1},
			}),
		)), "SELECT COUNT() FROM (SELECT number FROM system.numbers LIMIT 5)")
		if err := rows.Scan(&count); assert.NoError(t, err) {
			assert.Equal(t, uint64(5), count)
		}
	}
}
