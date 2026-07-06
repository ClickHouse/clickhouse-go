package clickhouse_api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// FormatCSV ingests a raw CSV payload with InsertFormat and streams the table
// back out as CSV with QueryFormat. Any io.Reader works as the insert source:
// a file, an HTTP body, or, as here, an in-memory string.
func FormatCSV() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS example_format_csv")

	if err := conn.Exec(ctx, `
		CREATE TABLE example_format_csv (
			  id    Int64
			, name  String
			, score Float64
		) Engine = Memory
	`); err != nil {
		return err
	}

	csvPayload := strings.NewReader("1,alice,3.5\n2,bob,-0.25\n3,\"carol, jr\",9.75\n")
	if err := conn.InsertFormat(ctx, "CSV", "INSERT INTO example_format_csv", csvPayload); err != nil {
		return err
	}

	// The stream holds a connection until closed.
	stream, err := conn.QueryFormat(ctx, "CSV",
		"SELECT id, name, score FROM example_format_csv WHERE score > ? ORDER BY id", 0)
	if err != nil {
		return err
	}
	defer stream.Close()

	payload, err := io.ReadAll(stream)
	if err != nil {
		return err
	}
	fmt.Print(string(payload))
	return nil
}

// FormatJSONEachRow streams query results as JSONEachRow and consumes them
// incrementally with encoding/json - no Rows, no Scan, and without buffering
// the whole result in memory.
func FormatJSONEachRow() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS example_format_json")

	if err := conn.Exec(ctx, `
		CREATE TABLE example_format_json (
			  event   String
			, user_id UInt64
			, payload Nullable(String)
		) Engine = Memory
	`); err != nil {
		return err
	}

	jsonPayload := strings.NewReader(`
		{"event":"login","user_id":1,"payload":"mobile"}
		{"event":"click","user_id":1,"payload":null}
		{"event":"login","user_id":2,"payload":"web"}
	`)
	if err := conn.InsertFormat(ctx, "JSONEachRow", "INSERT INTO example_format_json", jsonPayload); err != nil {
		return err
	}

	stream, err := conn.QueryFormat(ctx, "JSONEachRow",
		"SELECT event, user_id, payload FROM example_format_json ORDER BY user_id, event")
	if err != nil {
		return err
	}
	defer stream.Close()

	dec := json.NewDecoder(stream)
	for {
		var row struct {
			Event   string  `json:"event"`
			UserID  uint64  `json:"user_id"`
			Payload *string `json:"payload"`
		}
		if err := dec.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		payload := "<null>"
		if row.Payload != nil {
			payload = *row.Payload
		}
		fmt.Printf("event=%s user=%d payload=%s\n", row.Event, row.UserID, payload)
	}
	return nil
}

// FormatParquet exports a query result to a .parquet file and imports a
// .parquet file into a table - the two halves of a typical data-lake
// exchange. Over HTTP the server produces and parses the Parquet bytes; over
// the native protocol the built-in client-side codec does.
func FormatParquet() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer conn.Exec(ctx, "DROP TABLE IF EXISTS example_format_parquet")

	// Export: stream a query result into a Parquet file on disk.
	stream, err := conn.QueryFormat(ctx, "Parquet",
		"SELECT number AS id, concat('user-', toString(number)) AS name FROM numbers(1000)")
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "example-*.parquet")
	if err != nil {
		stream.Close()
		return err
	}
	defer os.Remove(file.Name())
	if _, err := io.Copy(file, stream); err != nil {
		stream.Close()
		file.Close()
		return err
	}
	if err := stream.Close(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	// Import: load the Parquet file into a table.
	if err := conn.Exec(ctx, `
		CREATE TABLE example_format_parquet (
			  id   UInt64
			, name String
		) Engine = Memory
	`); err != nil {
		return err
	}
	parquetFile, err := os.Open(file.Name())
	if err != nil {
		return err
	}
	defer parquetFile.Close()
	if err := conn.InsertFormat(ctx, "Parquet", "INSERT INTO example_format_parquet", parquetFile); err != nil {
		return err
	}

	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM example_format_parquet").Scan(&count); err != nil {
		return err
	}
	fmt.Printf("imported %d rows from a %s\n", count, "parquet file")
	return nil
}
