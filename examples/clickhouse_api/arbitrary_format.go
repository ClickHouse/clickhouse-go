package clickhouse_api

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// ArbitraryFormat demonstrates streaming data in and out of ClickHouse in any
// server format, without going through Rows or Batch. Over HTTP the server
// converts every format; over the native protocol the client-side codecs
// registered in Options.FormatCodecs (CSV, JSONEachRow, Parquet and
// ArrowStream are built in) do the conversion.
func ArbitraryFormat() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS example_arbitrary_format")

	if err := conn.Exec(ctx, `
		CREATE TABLE example_arbitrary_format (
			  id   Int64
			, name String
		) Engine = Memory
	`); err != nil {
		return err
	}

	// Insert pre-formatted CSV straight from any io.Reader - a file, an HTTP
	// body, or, as here, an in-memory string.
	csvPayload := "1,alice\n2,bob\n3,carol\n"
	if err := conn.InsertArbitraryFormat(ctx, "CSV",
		"INSERT INTO example_arbitrary_format", strings.NewReader(csvPayload)); err != nil {
		return err
	}

	// Stream the result back out as JSONEachRow. The stream holds a
	// connection until closed.
	stream, err := conn.QueryArbitraryFormat(ctx, "JSONEachRow",
		"SELECT id, name FROM example_arbitrary_format ORDER BY id")
	if err != nil {
		return err
	}
	jsonPayload, err := io.ReadAll(stream)
	if err != nil {
		stream.Close()
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	fmt.Print(string(jsonPayload))

	// Binary formats work the same way - here the result arrives as a
	// complete Parquet file.
	stream, err = conn.QueryArbitraryFormat(ctx, "Parquet",
		"SELECT id, name FROM example_arbitrary_format ORDER BY id")
	if err != nil {
		return err
	}
	parquetPayload, err := io.ReadAll(stream)
	if err != nil {
		stream.Close()
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}
	fmt.Printf("parquet result: %d bytes\n", len(parquetPayload))

	return nil
}
