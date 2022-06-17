package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	//conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000?dial_timeout=1s&compress=true")
	//if err != nil {
	//	return err
	//}
	err := example()
	if err != nil {
		panic(err)
	}

	fmt.Println("goooo")
	return
}

func example() error {
	conn, err := sql.Open("clickhousehttp", "http://127.0.0.1:8123?dial_timeout=1s&compress=true")
	if err != nil {
		return err
	}

	//conn.SetMaxOpenConns()
	fmt.Println(conn)
	fmt.Println(conn.Ping())
	//conn.SetMaxOpenConns()

	//const ddl = `
	//CREATE TABLE example2 (
	//	  Col1 UInt8
	//	, Col2 String
	//	, Col3 DateTime
	//) ENGINE = Memory
	//`
	//res, err := conn.Exec(ddl)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(err)
	//fmt.Println(res)

	// with params
	//const ins = "Insert into example2 (Col2, Col3) values(@ff, @h)"
	//res, err := conn.ExecContext(
	//	context.Background(), ins,
	//	//sql.Named("gf", 0),
	//	sql.Named("ff", "test"),
	//	sql.Named("h", time.Now()),
	//)
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(err)
	//fmt.Println(res)
	//
	//return nil
	const selectQ = "select Col1, Col2 from example2"

	rows, err := conn.QueryContext(
		context.Background(), selectQ,
		//sql.Named("gf", 0),
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(err)
	fmt.Println(rows)

	//var result struct {
	//	Col1 uint8
	//	Col2 string
	//	Col3 time.Time
	//}

	for rows.Next() {
		var (
			col1 uint8
			col2 string
			//col3 time.Time
		)
		if err := rows.Scan(&col1, &col2); err != nil {
			return err
		}
		fmt.Printf("row: col1=%d, col2=%s, col3=%s\n", col1, col2)
	}
	rows.Close()

	return nil
}
