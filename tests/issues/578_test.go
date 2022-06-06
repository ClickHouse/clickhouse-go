package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test578(t *testing.T) {
	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	assert.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO non_existent_table")
	assert.Error(t, err)

	if batch != nil {
		batch.Abort()
	}
}
