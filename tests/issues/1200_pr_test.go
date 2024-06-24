package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

func Test1200(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1200 (id UInt32, null_str Nullable(FixedString(5))) Engine MergeTree() ORDER BY tuple()"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1200")
	}()

	v := "value"

	tests := []struct {
		name  string
		value fmt.Stringer
		want  *string
	}{
		{
			name:  "fmt.Stringer implemented struct value",
			value: Test1200NullStr{underlying: v},
			want:  &v,
		},
		{
			name:  "nil value",
			value: nil,
			want:  nil,
		},
		{
			name:  "fmt.Stringer implemented struct pointer value",
			value: &Test1200NullStr{underlying: v},
			want:  &v,
		},
		{
			name:  "fmt.Stringer implemented struct typed-nil value",
			value: (*Test1200NullStr)(nil),
			want:  nil,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := i + 1
			err = conn.Exec(ctx, "INSERT INTO test_1200 (id, null_str) VALUES (?, ?)", id, tt.value)
			require.NoError(t, err)

			var got *string
			err = conn.QueryRow(ctx, "SELECT null_str FROM test_1200 WHERE id = ?", id).Scan(&got)
			require.NoError(t, err)

			if tt.want == nil {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, *tt.want, *got)
			}
		})
	}
}

type Test1200NullStr struct {
	underlying string
}

func (nc Test1200NullStr) String() string {
	return nc.underlying
}
