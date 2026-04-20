package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestContributors(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		if assert.NoError(t, err) {
			for _, contributor := range conn.Contributors() {
				t.Log(contributor)
			}
		}
	})
}
