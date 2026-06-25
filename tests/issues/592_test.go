package issues

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestIssue592(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{})
	assert.NoError(t, err)

	ctx := context.Background()
	err = conn.Exec(ctx, "DROP TABLE test_connection")
	assert.Error(t, err)
}
