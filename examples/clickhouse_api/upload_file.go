package clickhouse_api

import (
	"bytes"
	"context"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func UploadFile() error {
	conn, err := GetHTTPConnection("UploadFile", nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_upload_file")
	}()
	conn.Exec(ctx, `DROP TABLE IF EXISTS test_upload_file`)
	if err = conn.Exec(ctx, `
		CREATE TABLE test_upload_file (
				  File LowCardinality(FixedString(32))
			    , ID   UInt32
			    , Data Nullable(FixedString(32))
			) Engine MergeTree() ORDER BY (File, ID)
	`); err != nil {
		return err
	}

	filePath := "./../../tests/testdata/upload_file.tsv.zstd"
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx = clickhouse.Context(ctx, clickhouse.WithFileEncoding(filePath))
	err = conn.UploadFile(ctx, f, "INSERT INTO test_upload_file FORMAT TSV")
	if err != nil {
		return err
	}

	return nil
}

func UploadFileReader() error {
	conn, err := GetHTTPConnection("UploadFileReader", nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer func() {
		conn.Exec(ctx, "DROP TABLE test_upload_file")
	}()
	conn.Exec(ctx, `DROP TABLE IF EXISTS test_upload_file`)
	if err = conn.Exec(ctx, `
		CREATE TABLE test_upload_file (
				  File LowCardinality(FixedString(32))
			    , ID   UInt32
			    , Data Nullable(FixedString(32))
			) Engine MergeTree() ORDER BY (File, ID)
	`); err != nil {
		return err
	}

	const data = `{"File": "upload_file.json", "ID": 1, "Data": null}
{"File": "upload_file.json", "ID": 2, "Data": "ASD"}
{"File": "upload_file.json", "ID": 3, "Data": "ASD"}
{"File": "upload_file.json", "ID": 4, "Data": "QWE"}
{"File": "upload_file.json", "ID": 5, "Data": "Foo"}
`
	buf := bytes.NewBufferString(data)

	// The data is uncompressed, so the Encoding parameter value is empty. It's empty by default; we've set it explicitly for clarity only.
	// Content-Type is also set automatically based on the request. However, we can set it manually if necessary.
	ctx = clickhouse.Context(ctx, clickhouse.WithEncoding(""), clickhouse.WithFileContentType("application/json"))

	err = conn.UploadFile(ctx, buf, "INSERT INTO test_upload_file FORMAT JSONEachRow")
	if err != nil {
		return err
	}

	return nil
}
