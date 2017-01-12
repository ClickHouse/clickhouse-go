package clickhouse

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_Open(t *testing.T) {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123?username=username&password=password&timeout=90&debug=true&alt_hosts=127.0.0.1:8123,127.0.0.1:8124")
	if assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
	}
}

/*
CREATE TABLE stats
(
    app_id UInt32,
    language FixedString(2),
    country FixedString(2),
    date Date,
    datetime DateTime
) ENGINE = Memory
*/
func Test_Batch(t *testing.T) {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123?debug=true")
	if assert.NoError(t, err) {
		assert.NoError(t, connect.Ping())
	}
	if tx, err := connect.Begin(); assert.NoError(t, err) {
		if stmt, err := tx.Prepare("INSERT INTO stats (app_id, language, country, date, datetime) VALUES (?, ?, ?, ?, ?)"); assert.NoError(t, err) {
			for i := 0; i < 10; i++ {
				if _, err := stmt.Exec(1, "RU", "RU", time.Date(2017, 1, 12, 0, 0, 0, 0, time.UTC), time.Now()); assert.NoError(t, err) {
				}
			}
		}
		if stmt, err := tx.Prepare("INSERT INTO stats VALUES (?, ?, ?, ?, ?)"); assert.NoError(t, err) {
			for i := 0; i < 10; i++ {
				if _, err := stmt.Exec(1, "RU", "RU", time.Date(2017, 1, 12, 0, 0, 0, 0, time.UTC), time.Now()); assert.NoError(t, err) {
				}
			}
		}
		if err := tx.Commit(); assert.NoError(t, err) {
			assert.Equal(t, sql.ErrTxDone, tx.Rollback())
		}
	}
}

func Test_Query(t *testing.T) {
	connect, err := sql.Open("clickhouse", "http://127.0.0.1:8123?debug=true")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := connect.Query("SELECT app_id, language, country, date, datetime FROM stats WHERE app_id IN (?, ?, ?) LIMIT 20", 1, 2, 3)
	if assert.NoError(t, err) {
		t.Log(rows.Columns())
		for rows.Next() {
			var (
				appID             int
				language, country string
				date, datetime    time.Time
			)
			if err := rows.Scan(&appID, &language, &country, &date, &datetime); assert.NoError(t, err) {
				t.Logf("AppID: %d, language: %s, country: %s, date: %s, datetime: %s", appID, language, country, date, datetime)
			}
		}
	}
}
