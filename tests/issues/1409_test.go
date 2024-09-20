package issues

import (
	"context"
	"testing"
	"time"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1409(t *testing.T) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse_tests.GetConnection("issues", nil, nil, nil)
	)
	require.NoError(t, err)
	const ddl = `
		CREATE TABLE issue_1409 (
			  id UInt32,
				name String,
				birth_date Date,
				is_active Boolean
		) Engine = Memory
		`
	defer func() {
		conn.Exec(ctx, "DROP TABLE issue_1409")
	}()

	err = conn.Exec(ctx, ddl)
	require.NoError(t, err)

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO issue_1409")
	require.NoError(t, err)

	err = batch.Append(uint32(1), "Alice", time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC), true)
	require.NoError(t, err)
	// Append the second row
	err = batch.Append(uint32(2), "Bob", time.Date(1985, 12, 30, 0, 0, 0, 0, time.UTC), false)
	require.NoError(t, err)

	err = batch.Column(0).AppendRow(uint32(3))
	require.NoError(t, err)
	err = batch.Column(1).AppendRow("Charlie")
	require.NoError(t, err)
	err = batch.Column(2).AppendRow(time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	err = batch.Column(3).AppendRow(true)
	require.NoError(t, err)

	// test the issue_1409
	exampleId := []uint32{4, 5, 6}
	exampleName := []string{"Dave", "Eve", "Frank"}
	exampleBirthDate := []time.Time{time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC), time.Date(1985, 12, 30, 0, 0, 0, 0, time.UTC), time.Date(1990, 6, 15, 0, 0, 0, 0, time.UTC)}
	exampleIsActive := []bool{true, false, true}

	err = batch.Column(0).Append(exampleId)
	require.NoError(t, err)
	err = batch.Column(1).Append(exampleName)
	require.NoError(t, err)
	err = batch.Column(2).Append(exampleBirthDate)
	require.NoError(t, err)
	err = batch.Column(3).Append(exampleIsActive)
	require.NoError(t, err)

	err = batch.Send()
	require.NoError(t, err)
}
