package main

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type DatabaseFrame struct {
	name        string
	ColumnNames []string
	rows        *sql.Rows
	columnTypes []*sql.ColumnType
	vars        []interface{}
}

func NewDatabaseFrame(name string, rows *sql.Rows) (DatabaseFrame, error) {
	databaseFrame := DatabaseFrame{}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return DatabaseFrame{}, err
	}
	databaseFrame.columnTypes = columnTypes
	databaseFrame.name = name
	vars := make([]interface{}, len(columnTypes), len(columnTypes))
	columnNames := make([]string, len(columnTypes), len(columnTypes))
	for i := range columnTypes {
		value := reflect.Zero(columnTypes[i].ScanType()).Interface()
		vars[i] = &value
		columnNames[i] = columnTypes[i].Name()
	}
	databaseFrame.ColumnNames = columnNames
	databaseFrame.vars = vars
	databaseFrame.rows = rows
	return databaseFrame, nil
}

func (f DatabaseFrame) Next() ([]interface{}, bool, error) {
	values := make([]interface{}, len(f.columnTypes), len(f.columnTypes))
	for f.rows.Next() {
		if err := f.rows.Scan(f.vars...); err != nil {
			return nil, false, err
		}
		for i := range f.columnTypes {
			ptr := reflect.ValueOf(f.vars[i])
			values[i] = ptr.Elem().Interface()
		}
		return values, true, nil
	}
	f.rows.Close()
	return nil, false, f.rows.Err()
}

func NewNativeClient(host string, port uint16, username string, password string) (*sql.DB, error) {
	// debug output ?debug=true
	connection, err := sql.Open("clickhouse", fmt.Sprintf("clickhouse://%s:%s@%s:%d/", username, password, host, port))
	if err != nil {
		return nil, err
	}
	if err := connection.Ping(); err != nil {
		return nil, err
	}
	return connection, nil
}

func main() {
	c, err := NewNativeClient("localhost", 9000, "", "")
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	log.Printf("Reading system.%s", "system.query_thread_log")
	rows, err := c.Query("SELECT * FROM system.query_thread_log")
	if err != nil {
		log.Printf("Query failed")
		log.Fatal(err)
	}
	frame, err := NewDatabaseFrame("db_frame", rows)
	if err != nil {
		log.Println("Cant' construct frame")
		log.Fatal(err)
	}
	//iterate to exhaustion

	for {
		_, ok, err := frame.Next()
		if !ok {
			if err != nil {
				log.Println("Failed on termination")
				log.Println(err)
				break
			}
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		i++
	}
	log.Printf("Success with %d rows!!", i)
}
