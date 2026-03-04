package issues

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"

	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	"github.com/stretchr/testify/require"
)

type SomeStruct struct {
	SomeField string `json:"some_field"`
}

// SomeStructs implements sql.Scanner and driver.Valuer interfaces.
// We want to save slice as a JSON object to clickhouse. We're using Nullable(String) for this purpose.
type SomeStructs []SomeStruct

func (ss *SomeStructs) Scan(src any) error {
	if src == nil {
		*ss = nil
		return nil
	}

	sp, ok := src.(string)
	if !ok {
		return fmt.Errorf("expected *string, got %T", src)
	}

	return json.Unmarshal([]byte(sp), ss)
}

func (ss SomeStructs) Value() (driver.Value, error) {
	if ss == nil {
		return nil, nil
	}

	marshalled, err := json.Marshal(ss)
	if err != nil {
		return nil, err
	}

	return string(marshalled), nil
}

func Test1359(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment("issues")
	require.NoError(t, err)
	conn, err := clickhouse_tests.TestDatabaseSQLClientWithDefaultSettings(testEnv)
	require.NoError(t, err)

	_, err = conn.Exec(`CREATE TABLE test_1359 (
			  NStr Nullable(String)
		) Engine MergeTree() ORDER BY tuple()`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = conn.Exec("DROP TABLE test_1359")
	})

	toInsert := SomeStructs{
		{SomeField: "value1"},
		{SomeField: "value2"},
	}
	_, err = conn.Exec("insert into test_1359 (NStr) values (?)", toInsert)
	require.NoError(t, err)

	var fromDB SomeStructs

	row := conn.QueryRow("select NStr from test_1359 limit 1")
	require.NoError(t, row.Scan(&fromDB))
}
