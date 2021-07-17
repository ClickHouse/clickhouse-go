package clickhouse

import (
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_NullableArray(t *testing.T) {
	const (
		ddl = `
			CREATE TABLE clickhouse_test_nullable_array (
				arr_int8     Array(Nullable(Int8))
			) Engine=Memory;
		`
		query = `
			SELECT
				arr_int8
			FROM clickhouse_test_nullable_array
		`
	)
	if connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true"); assert.NoError(t, err) {
		if _, err := connect.Begin(); assert.NoError(t, err) {
			if _, err := connect.Exec("DROP TABLE IF EXISTS clickhouse_test_nullable"); assert.NoError(t, err) {
				if rows, err := connect.Query(query); assert.NoError(t, err) {
					for rows.Next() {
						var (
							ArrInt8 = make([]*int8, 0)
						)
						if err := rows.Scan(
							&ArrInt8,
						); assert.NoError(t, err) {
							fmt.Printf("ok")
						}
					}
				}
			}
		}
	}
}
