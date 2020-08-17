package main

import (
	"database/sql"
	_ "github.com/ClickHouse/clickhouse-go"
	"log"
	"reflect"
)

func main() {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := connect.Query(`select CAST(null as Nullable(Int64))`)
	if err != nil {
		log.Fatal(err)
	}

	columns, err := rows.ColumnTypes()
	values := make([]interface{}, len(columns))
	for i, c := range columns {
		values[i] = reflect.New(c.ScanType()).Interface()
	}

	rows.Next()
	err = rows.Scan(values...)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(reflect.TypeOf(values[0]).String())
}
