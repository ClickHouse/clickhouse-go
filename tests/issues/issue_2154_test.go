package issues

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
	clickhouse_std_tests "github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

// names2154 and times2154 are the values of each row. The third time is in the same
// second as the first, so the `DateTime` columns store the same value for both.
var (
	names2154 = []string{"apple", "banana", "apple", "cherry"}
	start2154 = time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	times2154 = []time.Time{
		start2154,
		start2154.Add(time.Second),
		start2154.Add(500 * time.Millisecond),
		start2154.Add(2 * time.Second),
	}
)

type event2154 struct {
	ID   uint8      `ch:"id" json:"id"`
	S    *string    `ch:"s" json:"s"`
	LC   *string    `ch:"lc" json:"lc"`
	LCN  *string    `ch:"lcn" json:"lcn"`
	DT   *time.Time `ch:"dt" json:"dt"`
	LCDT *time.Time `ch:"lcdt" json:"lcdt"`
}

// Test2154 verifies that LowCardinality columns store the value of each row when the
// caller appends one pointer for all rows and changes the value it points to.
// See https://github.com/ClickHouse/clickhouse-go/issues/2154.
func Test2154(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)

	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		ctx := context.Background()

		t.Run("Append", func(t *testing.T) {
			tableName := fmt.Sprintf("test_2154_append_%v", protocol)
			createTable2154(t, conn, tableName)

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
			require.NoError(t, err)
			var (
				name string
				ts   time.Time
			)
			for i := range names2154 {
				name, ts = names2154[i], times2154[i]
				require.NoError(t, batch.Append(uint8(i), &name, &name, &name, &ts, &ts))
			}
			require.NoError(t, batch.Send())

			requireRows2154(t, conn, tableName)
		})

		t.Run("AppendStruct", func(t *testing.T) {
			tableName := fmt.Sprintf("test_2154_struct_%v", protocol)
			createTable2154(t, conn, tableName)

			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableName)
			require.NoError(t, err)
			// json.Unmarshal writes to the pointers that the struct already has, so all
			// rows after the first use the same pointers.
			var event event2154
			for i := range names2154 {
				line := fmt.Sprintf(`{"id":%d,"s":%[2]q,"lc":%[2]q,"lcn":%[2]q,"dt":%[3]q,"lcdt":%[3]q}`,
					i, names2154[i], times2154[i].Format(time.RFC3339Nano))
				require.NoError(t, json.Unmarshal([]byte(line), &event))
				require.NoError(t, batch.AppendStruct(&event))
			}
			require.NoError(t, batch.Send())

			requireRows2154(t, conn, tableName)
		})
	})
}

// Test2154Std is Test2154 for the database/sql API.
func Test2154Std(t *testing.T) {
	testEnv, err := clickhouse_tests.GetTestEnvironment(testSet)
	require.NoError(t, err)
	useSSL, err := strconv.ParseBool(clickhouse_tests.GetEnv("CLICKHOUSE_USE_SSL", "false"))
	require.NoError(t, err)

	clickhouse_tests.TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		db, err := clickhouse_std_tests.GetDSNConnection(testSet, protocol, useSSL, nil)
		require.NoError(t, err)
		t.Cleanup(func() { db.Close() })
		conn, err := clickhouse_tests.GetConnection(testSet, t, protocol, clickhouse_tests.TestClientDefaultSettings(testEnv), nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })

		tableName := fmt.Sprintf("test_2154_std_%v", protocol)
		createTable2154(t, conn, tableName)

		tx, err := db.Begin()
		require.NoError(t, err)
		stmt, err := tx.Prepare("INSERT INTO " + tableName)
		require.NoError(t, err)
		var (
			name string
			ts   time.Time
		)
		for i := range names2154 {
			name, ts = names2154[i], times2154[i]
			_, err := stmt.Exec(uint8(i), &name, &name, &name, &ts, &ts)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Commit())

		requireRows2154(t, conn, tableName)
	})
}

func createTable2154(t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()
	ctx := context.Background()
	// `LowCardinality(DateTime)` needs `allow_suspicious_low_cardinality_types`.
	createCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"allow_suspicious_low_cardinality_types": 1,
	}))
	require.NoError(t, conn.Exec(createCtx, fmt.Sprintf(`CREATE TABLE %s (
		id   UInt8,
		s    String,
		lc   LowCardinality(String),
		lcn  LowCardinality(Nullable(String)),
		dt   DateTime('UTC'),
		lcdt LowCardinality(DateTime('UTC'))
	) ENGINE = MergeTree ORDER BY tuple()`, tableName)))
	t.Cleanup(func() {
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+tableName); err != nil {
			t.Logf("DROP TABLE %s failed: %v", tableName, err)
		}
	})
}

// requireRows2154 reads the table and compares each row with names2154 and times2154.
func requireRows2154(t *testing.T, conn driver.Conn, tableName string) {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT id, s, lc, lcn, dt, lcdt FROM "+tableName+" ORDER BY id")
	require.NoError(t, err)
	t.Cleanup(func() { rows.Close() })

	var count int
	for rows.Next() {
		var (
			id       uint8
			s, lc    string
			lcn      *string
			dt, lcdt time.Time
		)
		require.NoError(t, rows.Scan(&id, &s, &lc, &lcn, &dt, &lcdt))
		require.Less(t, int(id), len(names2154))
		name, ts := names2154[id], times2154[id].Truncate(time.Second)
		assert.Equal(t, name, s, "row %d: String", id)
		assert.Equal(t, name, lc, "row %d: LowCardinality(String)", id)
		if assert.NotNil(t, lcn, "row %d: LowCardinality(Nullable(String))", id) {
			assert.Equal(t, name, *lcn, "row %d: LowCardinality(Nullable(String))", id)
		}
		assert.Equal(t, ts.Unix(), dt.Unix(), "row %d: DateTime", id)
		assert.Equal(t, ts.Unix(), lcdt.Unix(), "row %d: LowCardinality(DateTime)", id)
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, len(names2154), count)
}
