package std

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStdDateTime64(t *testing.T) {
	if conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000"); assert.NoError(t, err) {
		if err := checkMinServerVersion(conn, 20, 3); err != nil {
			t.Skip(err.Error())
			return
		}
		const ddl = `
			CREATE TEMPORARY TABLE test_datetime64 (
				  Col1 DateTime64(3)
				, Col2 DateTime64(9, 'Europe/Moscow')
				, Col3 DateTime64(0, 'Europe/London')
				, Col4 Nullable(DateTime64(3, 'Europe/Moscow'))
				, Col5 Array(DateTime64(3, 'Europe/Moscow'))
				, Col6 Array(Nullable(DateTime64(3, 'Europe/Moscow')))
			)
		`
		if _, err := conn.Exec(ddl); assert.NoError(t, err) {
			scope, err := conn.Begin()
			if !assert.NoError(t, err) {
				return
			}
			if batch, err := scope.Prepare("INSERT INTO test_datetime64"); assert.NoError(t, err) {
				var (
					datetime1 = time.Now().Truncate(time.Millisecond)
					datetime2 = time.Now().Truncate(time.Nanosecond)
					datetime3 = time.Now().Truncate(time.Second)
				)
				if _, err := batch.Exec(
					datetime1,
					datetime2,
					datetime3,
					&datetime1,
					[]time.Time{datetime1, datetime1},
					[]*time.Time{&datetime3, nil, &datetime3},
				); assert.NoError(t, err) {
					if err := scope.Commit(); assert.NoError(t, err) {
						var (
							col1 time.Time
							col2 time.Time
							col3 time.Time
							col4 *time.Time
							col5 []time.Time
							col6 []*time.Time
						)
						if err := conn.QueryRow("SELECT * FROM test_datetime64").Scan(&col1, &col2, &col3, &col4, &col5, &col6); assert.NoError(t, err) {
							assert.Equal(t, datetime1, col1)
							assert.Equal(t, datetime2.UnixNano(), col2.UnixNano())
							assert.Equal(t, datetime3.UnixNano(), col3.UnixNano())
							if assert.Equal(t, "Europe/Moscow", col2.Location().String()) {
								assert.Equal(t, "Europe/London", col3.Location().String())
							}
							assert.Equal(t, datetime1.UnixNano(), col4.UnixNano())
							if assert.Len(t, col5, 2) {
								assert.Equal(t, "Europe/Moscow", col5[0].Location().String())
								assert.Equal(t, "Europe/Moscow", col5[1].Location().String())
							}
							if assert.Len(t, col6, 3) {
								assert.Nil(t, col6[1])
								assert.NotNil(t, col6[0])
								assert.NotNil(t, col6[2])
							}
						}
					}
				}
			}
		}
	}
}
