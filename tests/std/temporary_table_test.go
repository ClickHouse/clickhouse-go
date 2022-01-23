package std

import (
	"database/sql"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/assert"
)

func TestStdTemporaryTable(t *testing.T) {
	const (
		ddl = `
			CREATE TEMPORARY TABLE clickhouse_test_temporary_table (
				ID UInt64
			);
		`
	)
	if connect, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if tx, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := tx.Exec(ddl); assert.NoError(t, err) {
				if _, err := tx.Exec("INSERT INTO clickhouse_test_temporary_table (ID) SELECT number AS ID FROM system.numbers LIMIT 10"); assert.NoError(t, err) {
					if rows, err := tx.Query("SELECT ID AS ID FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
						var count int
						for rows.Next() {
							var num int
							if err := rows.Scan(&num); !assert.NoError(t, err) {
								return
							}
							count++
						}
						if _, err = tx.Query("SELECT ID AS ID1 FROM clickhouse_test_temporary_table"); assert.NoError(t, err) {
							if _, err = connect.Query("SELECT ID AS ID2 FROM clickhouse_test_temporary_table"); assert.Error(t, err) {
								if exception, ok := err.(*clickhouse.Exception); assert.True(t, ok) {
									assert.Equal(t, int32(60), exception.Code)
								}
							}
						}
						if assert.Equal(t, int(10), count) {
							if assert.NoError(t, tx.Commit()) {
								assert.NoError(t, connect.Close())
							}
						}
					}
				}
			}
		}
	}
}
