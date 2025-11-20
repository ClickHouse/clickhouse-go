package tests

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestInterval(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		const query = `
		SELECT
			  INTERVAL 1 SECOND
			, INTERVAL 4 SECOND
			, INTERVAL 1 MINUTE
			, INTERVAL 5 MINUTE
		`
		var (
			col1 string
			col2 string
			col3 string
			col4 string
		)
		err = conn.QueryRow(ctx, query).Scan(
			&col1,
			&col2,
			&col3,
			&col4,
		)
		require.NoError(t, err)
		assert.Equal(t, "1 Second", col1)
		assert.Equal(t, "4 Seconds", col2)
		assert.Equal(t, "1 Minute", col3)
		assert.Equal(t, "5 Minutes", col4)
	})
}
