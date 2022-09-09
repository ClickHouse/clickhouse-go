package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test592(t *testing.T) {
	conn, err := clickhouse_tests.GetConnectionWithOptions(&clickhouse.Options{})
	assert.NoError(t, err)

	ctx := context.Background()
	err = conn.Exec(ctx, "DROP TABLE test_connection")
	assert.Error(t, err)
}
