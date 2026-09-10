package std

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/tests/std"
)

func QueryWithParameters() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	if err != nil {
		return err
	}

	if !std.CheckMinServerVersion(conn, 22, 8, 0) {
		return nil
	}

	// A string passed via Named is treated as the literal value and the driver
	// escapes control characters automatically, so the value round-trips
	// byte-for-byte — including an actual newline, tab, or backslash.
	row := conn.QueryRow(
		`SELECT
			{column:Identifier},
			{str:String},
			{array:Array(String)},
			{control:String},
			{tab:String},
			{backslash:String}
		FROM {database:Identifier}.{table:Identifier}
		LIMIT 1 OFFSET 100`,
		clickhouse.Named("str", "hello"),
		clickhouse.Named("array", "['a', 'b', 'c']"),
		clickhouse.Named("column", "number"),
		clickhouse.Named("database", "system"),
		clickhouse.Named("table", "numbers"),
		clickhouse.Named("control", "line 1\nline 2\tend"),
		clickhouse.Named("tab", "column 1\tcolumn 2"),
		clickhouse.Named("backslash", `C:\Users\bob`),
	)
	var (
		column    uint64
		str       string
		array     []string
		control   string
		tab       string
		backslash string
	)
	if err := row.Scan(&column, &str, &array, &control, &tab, &backslash); err != nil {
		return err
	}
	fmt.Printf("row: column=%d, str=%s, array=%s, control=%q, tab=%q, backslash=%q\n",
		column, str, array, control, tab, backslash)
	return nil
}
