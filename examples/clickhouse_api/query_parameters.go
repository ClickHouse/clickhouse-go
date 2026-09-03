package clickhouse_api

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func QueryWithParameters() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}

	if !clickhouse_tests.CheckMinServerServerVersion(conn, 22, 8, 0) {
		return nil
	}

	// ClickHouse parses parameter strings in its Escaped format. A raw Go
	// literal preserves the backslashes; an interpreted literal needs an
	// extra backslash. Double the Escaped backslash to receive a literal \n.
	chCtx := clickhouse.Context(context.Background(), clickhouse.WithParameters(clickhouse.Parameters{
		"str":                 "hello",
		"array":               "['a', 'b', 'c']",
		"column":              "number",
		"database":            "system",
		"table":               "numbers",
		"escaped_raw":         `line 1\nline 2\tend`,
		"escaped_interpreted": "line 1\\nline 2\\tend",
		"literal_backslash":   `line 1\\nline 2`,
	}))

	row := conn.QueryRow(chCtx, `
		SELECT
			{column:Identifier},
			{str:String},
			{array:Array(String)},
			{escaped_raw:String},
			{escaped_interpreted:String},
			{literal_backslash:String}
		FROM {database:Identifier}.{table:Identifier}
		LIMIT 1 OFFSET 100`)
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
