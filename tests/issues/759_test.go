package issues

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test759(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	require.NoError(t, err)
	timeWant, err := time.Parse(time.RFC3339Nano, "2022-09-15T17:06:31.81718722+04:00")
	require.NoError(t, err)
	testWith(t, conn, timeWant.Local())
	testWith(t, conn, timeWant)

}

func testWith(t *testing.T, conn driver.Conn, timeWant time.Time) {
	date := clickhouse.DateNamed("Time", timeWant, clickhouse.NanoSeconds)
	r := conn.QueryRow(context.TODO(), "SELECT @Time", date)

	var timeGot time.Time
	require.NoError(t, r.Scan(&timeGot))
	require.Equal(t, timeGot.Unix(), timeWant.Unix())
}
