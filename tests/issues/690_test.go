package issues

import (
	"database/sql"
	"github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test690(t *testing.T) {
	conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000")
	require.NoError(t, err)
	if err := std.CheckMinServerVersion(conn, 21, 9, 0); err != nil {
		t.Skip(err.Error())
		return
	}
	const ddl = `
		CREATE TABLE test_map (
			  Col1 Map(String, UInt64)
			, Col2 Map(String, UInt64)
			, Col3 Map(String, UInt64)
			, Col4 Array(Map(String, String))
			, Col5 Map(LowCardinality(String), LowCardinality(UInt64))
			, Col6 Map(String, Map(String, Int64))
		) Engine Memory
		`

}
