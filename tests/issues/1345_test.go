package issues

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func TestIssue1345(t *testing.T) {
	ctx := context.Background()

	conn, err := tests.GetConnection("issues", nil, nil, nil)
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS `_test_1345#$.ДБ`"))
	defer conn.Exec(ctx, "DROP TABLE `_test_1345#$.ДБ`")

	require.NoError(t, conn.Exec(ctx, "CREATE  TABLE  IF NOT EXISTS `_test_1345#$.ДБ`.`2. Таблица №2` (i UInt64, s String) ENGINE = Memory()"))

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO `_test_1345#$.ДБ`.`2. Таблица №2`")
	require.NoError(t, err)

	var (
		i = uint64(32)
		s = "b"
	)

	err = batch.Append(i, s)
	require.NoError(t, err)

	err = batch.Send()
	require.NoError(t, err)

	rows, err := conn.Query(ctx, "SELECT * FROM `_test_1345#$.ДБ`.`2. Таблица №2`")
	require.NoError(t, err)

	require.True(t, rows.Next())

	var (
		actualInt uint64
		actualStr string
	)
	err = rows.Scan(&actualInt, &actualStr)
	require.NoError(t, err)

	require.Equal(t, i, actualInt)
	require.Equal(t, s, actualStr)

	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
}
