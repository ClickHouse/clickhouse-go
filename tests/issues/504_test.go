package issues

import (
	"context"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestIssue504(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnectionTCP("issues", nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	require.NoError(t, err)
	var result []struct {
		Col1 string
		Col2 uint64
	}
	const query = `
		SELECT *
		FROM
		(
			SELECT
				'A'    AS Col1,
				number AS Col2
			FROM
			(
				SELECT number
				FROM system.numbers
				LIMIT 5
			)
		)
		WHERE (Col1, Col2) IN (@GS)
		`
	err = conn.Select(ctx, &result, query, clickhouse.Named("GS", []clickhouse.GroupSet{
		{Value: []any{"A", 2}},
		{Value: []any{"A", 4}},
	}))
	require.NoError(t, err)
	assert.Equal(t, []struct {
		Col1 string
		Col2 uint64
	}{
		{
			Col1: "A",
			Col2: 2,
		},
		{
			Col1: "A",
			Col2: 4,
		},
	}, result)
}
