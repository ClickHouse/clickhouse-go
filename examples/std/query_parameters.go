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

	// ClickHouse parses parameter strings in its Escaped format. A raw Go
	// literal preserves the backslashes; an interpreted literal needs an
	// extra backslash. Double the Escaped backslash to receive a literal \n.
	row := conn.QueryRow(
		`SELECT
			{column:Identifier},
			{str:String},
			{array:Array(String)},
			{escaped_raw:String},
			{escaped_interpreted:String},
			{literal_backslash:String}
		FROM {database:Identifier}.{table:Identifier}
		LIMIT 1 OFFSET 100`,
		clickhouse.Named("str", "hello"),
		clickhouse.Named("array", "['a', 'b', 'c']"),
		clickhouse.Named("column", "number"),
		clickhouse.Named("database", "system"),
		clickhouse.Named("table", "numbers"),
		clickhouse.Named("escaped_raw", `line 1\nline 2\tend`),
		clickhouse.Named("escaped_interpreted", "line 1\\nline 2\\tend"),
		clickhouse.Named("literal_backslash", `line 1\\nline 2`),
	)
	var (
		column             uint64
		str                string
		array              []string
		escapedRaw         string
		escapedInterpreted string
		literalBackslash   string
	)
	if err := row.Scan(&column, &str, &array, &escapedRaw, &escapedInterpreted, &literalBackslash); err != nil {
		return err
	}
	fmt.Printf("row: column=%d, str=%s, array=%s, escapedRaw=%q, escapedInterpreted=%q, literalBackslash=%q\n",
		column, str, array, escapedRaw, escapedInterpreted, literalBackslash)
	return nil
}
