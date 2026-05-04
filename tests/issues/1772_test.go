package issues

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func Test1772_HTTPNativeCompressionMethodSettings(t *testing.T) {
	testCases := []struct {
		name           string
		method         clickhouse.CompressionMethod
		level          int
		expectedMethod string
	}{
		{
			name:           "zstd",
			method:         clickhouse.CompressionZSTD,
			level:          9,
			expectedMethod: "ZSTD",
		},
		{
			name:           "lz4",
			method:         clickhouse.CompressionLZ4,
			level:          3,
			expectedMethod: "LZ4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := clickhouse_tests.GetConnectionHTTP("issues", t.Name(), nil, nil, &clickhouse.Compression{
				Method: tc.method,
				Level:  tc.level,
			})
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, conn.Close())
			})

			ctx := context.Background()

			var method string
			require.NoError(t, conn.QueryRow(ctx, "SELECT getSetting('network_compression_method')").Scan(&method))
			require.Equal(t, tc.expectedMethod, strings.ToUpper(method))

			if tc.method == clickhouse.CompressionZSTD {
				var level int8
				require.NoError(t, conn.QueryRow(ctx, "SELECT getSetting('network_zstd_compression_level')").Scan(&level))
				require.Equal(t, int8(tc.level), level)
			}
		})
	}
}
