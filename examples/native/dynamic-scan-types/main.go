package main

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func example() error {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return err
	}
	const query = `
	SELECT
		   1     AS Col1
		, 'Text' AS Col2
	`
	rows, err := conn.Query(context.TODO(), query)
	if err != nil {
		return err
	}
	var (
		columnTypes = rows.ColumnTypes()
		vars        = make([]interface{}, len(columnTypes))
	)
	for i := range columnTypes {
		value := reflect.New(columnTypes[i].ScanType()).Interface()
		vars[i] = value
	}
	for rows.Next() {
		if err := rows.Scan(vars...); err != nil {
			return err
		}
		for _, v := range vars {
			switch v := v.(type) {
			case *string:
				fmt.Println(*v)
			case *uint8:
				fmt.Println(*v)
			}
		}
	}
	return nil
}
func main() {
	if err := example(); err != nil {
		log.Fatal(err)
	}
}
