package issues

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
	"github.com/stretchr/testify/require"
	"strconv"
	"testing"
)

func Test813(t *testing.T) {
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)
	conn, err := clickhouse_std_tests.GetDSNConnection("issues", clickhouse.Native, useSSL, nil)
	const ddl = `
		CREATE TABLE test_813 (
		  	IntValue Int64,
			Exemplars Nested (
				Attributes Map(LowCardinality(String), String)
			) CODEC(ZSTD(1)) 
		) Engine MergeTree() ORDER BY tuple()
		`
	conn.Exec("DROP TABLE test_813")
	defer func() {
		conn.Exec("DROP TABLE test_813")
	}()
	_, err = conn.Exec(ddl)
	require.NoError(t, err)

	valueArgs := []interface{}{
		int64(14),
		clickhouse.ArraySet{map[string]string{"array1_key1": "array1_value2", "array1_key2": "array1_value2"}},
	}
	_, err = conn.Exec("INSERT INTO test_813 (IntValue, Exemplars.Attributes) VALUES (?,?)", valueArgs...)
	require.NoError(t, err)
}
