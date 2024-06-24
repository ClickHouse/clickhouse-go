package issues

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

const (
	A SomeUint64AsString = iota + 1
	B
	C
)

type SomeUint64AsString uint64

func (f *SomeUint64AsString) Scan(src any) error {
	if t, ok := src.(uint64); ok {
		*f = SomeUint64AsString(t)
		return nil
	}
	if t, ok := src.(string); ok {
		switch t {
		case "A":
			*f = A
		case "B":
			*f = B
		case "C":
			*f = C
		default:
			return fmt.Errorf("cannot scan %s into SomeUint64AsString", t)
		}
		return nil
	}

	return fmt.Errorf("cannot scan %T into SomeUint64AsString", src)
}

func (f SomeUint64AsString) String() string {
	switch f {
	case A:
		return "A"
	case B:
		return "B"
	case C:
		return "C"
	}
	return ""
}

type Test struct {
	Col1 []SomeUint64AsString `ch:"Col1"`
}

func Test1128(t *testing.T) {
	var (
		conn, err = clickhouse_tests.GetConnection("issues", clickhouse.Settings{
			"max_execution_time": 60,
		}, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
	)
	ctx := context.Background()
	require.NoError(t, err)
	const ddl = "CREATE TABLE test_1128 (Col1 Array(String)) Engine MergeTree() ORDER BY tuple()"
	require.NoError(t, conn.Exec(ctx, ddl))
	defer func() {
		conn.Exec(ctx, "DROP TABLE IF EXISTS test_1128")
	}()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO test_1128")
	require.NoError(t, err)

	data := Test{
		Col1: []SomeUint64AsString{A, B, C},
	}
	require.NoError(t, batch.AppendStruct(&data))
	require.NoError(t, batch.Send())

	var res Test
	require.NoError(t, conn.QueryRow(ctx, "SELECT * FROM test_1128").ScanStruct(&res))
	require.Equal(t, []SomeUint64AsString{A, B, C}, res.Col1)
}
