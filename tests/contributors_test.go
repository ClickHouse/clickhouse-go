package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Contributors is retained for backwards compatibility but always returns an
// empty slice. See clickhouse.Contributors.
func TestContributors(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{})
	if !assert.NoError(t, err) {
		return
	}
	defer conn.Close()

	assert.Empty(t, conn.Contributors())
}
