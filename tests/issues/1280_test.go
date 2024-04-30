package issues

import (
	"context"
	"reflect"
	"regexp"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1280(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time":             60,
			"allow_experimental_object_type": true,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1280_values (`id` Int32) Engine = Memory"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1280_values")
	}()

	_, err = conn.PrepareBatch(context.Background(), "INSERT INTO test_1280_values")
	require.NoError(t, err)
}

func Test1280SplitInsertRe(t *testing.T) {
	var splitInsertRe = regexp.MustCompile(`(?i)\sVALUES\s*\(?`)

	// 定义测试用例
	testCases := []struct {
		input    string
		expected []string
	}{
		{
			input:    "INSERT INTO table_name VALUES (1, 'hello')",
			expected: []string{"INSERT INTO table_name", "1, 'hello')"},
		},
		{
			input:    "INSERT INTO table_name  values  (1, 'hello')",
			expected: []string{"INSERT INTO table_name ", "1, 'hello')"},
		},
		{
			input:    "INSERT INTO table_name\tVALUES\t(2, 'world')",
			expected: []string{"INSERT INTO table_name", "2, 'world')"},
		},
		{
			input:    "INSERT INTO table_name\t\tVALUES\t\t(2, 'world')",
			expected: []string{"INSERT INTO table_name\t", "2, 'world')"},
		},
		{
			input:    "INSERT INTO table_name \tVALUES\t(2, 'world')",
			expected: []string{"INSERT INTO table_name ", "2, 'world')"},
		},
		{
			input:    "INSERT INTO table_name\t VALUES\t(2, 'world')",
			expected: []string{"INSERT INTO table_name\t", "2, 'world')"},
		},
		{
			input:    "INSERT INTO table_name\nVALUES\n(3, 'foo')",
			expected: []string{"INSERT INTO table_name", "3, 'foo')"},
		},
		{
			input:    "INSERT INTO table_name\rVALUES\r(4, 'bar')",
			expected: []string{"INSERT INTO table_name", "4, 'bar')"},
		},
		{
			input:    "INSERT INTO table_name VALUES",
			expected: []string{"INSERT INTO table_name", ""},
		},
		{
			input:    "INSERT INTO table_name ",
			expected: []string{"INSERT INTO table_name "},
		},
		{
			input:    "INSERT INTO table_name",
			expected: []string{"INSERT INTO table_name"},
		},
		{
			input:    "INSERT INTO table_values",
			expected: []string{"INSERT INTO table_values"},
		},
	}

	// 遍历测试用例并执行测试
	for _, tc := range testCases {
		result := splitInsertRe.Split(tc.input, -1)
		if !reflect.DeepEqual(result, tc.expected) {
			t.Errorf("Input: %q, \nExpected: %v Got: %v", tc.input, tc.expected, result)
		}
	}
}
