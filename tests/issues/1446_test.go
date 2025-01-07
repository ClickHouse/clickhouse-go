package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sampleFailTuple struct {
	TupleOne string `ch:"tuple_one"`
	TupleTwo string `ch:"tuple_two"`
}

type sampleFailRow struct {
	MyId    uint64          `ch:"my_id"`
	MyTuple sampleFailTuple `ch:"my_tuple"`
}

type sampleOkTuple struct {
	TupleOne *string `ch:"tuple_one"`
	TupleTwo *string `ch:"tuple_two"`
}

type sampleOkRow struct {
	MyId    uint64        `ch:"my_id"`
	MyTuple sampleOkTuple `ch:"my_tuple"`
}

func TestIssue1446(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	const ddl = `
		CREATE TABLE IF NOT EXISTS issue_1446(
			my_id    UInt64,
			my_tuple Tuple(tuple_one Nullable(String), tuple_two Nullable(String))
		) ENGINE = MergeTree PRIMARY KEY (my_id) ORDER BY (my_id)
	`
	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	defer conn.Exec(ctx, "DROP TABLE issue_1446")

	err = conn.Exec(ctx, "INSERT INTO issue_1446(my_id, my_tuple) VALUES (1, tuple('one', 'two'))")
	require.NoError(t, err)

	failRow := sampleFailRow{}
	err = conn.QueryRow(ctx, "SELECT * FROM issue_1446 LIMIT 1").ScanStruct(&failRow)
	assert.EqualError(t, err, "clickhouse [ScanRow]: (my_tuple) converting *string to string is unsupported")

	okRow := sampleOkRow{}
	err = conn.QueryRow(ctx, "SELECT * FROM issue_1446 LIMIT 1").ScanStruct(&okRow)
	require.NoError(t, err)

	assert.Equal(t, "one", *okRow.MyTuple.TupleOne)
	assert.Equal(t, "two", *okRow.MyTuple.TupleTwo)
}
