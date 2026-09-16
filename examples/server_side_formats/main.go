package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const sourceQuery = `
	SELECT
		toUInt64(number + 1) AS id,
		arrayElement(['alice', 'bob', 'carol'], number + 1) AS name,
		toInt64((number + 1) * 10) AS score
	FROM numbers(3)
	ORDER BY id`

type formatCase struct {
	name      string
	extension string
	table     string
}

var formats = []formatCase{
	{name: "CSV", extension: ".csv", table: "clickhouse_go_server_format_demo_csv"},
	{name: "Parquet", extension: ".parquet", table: "clickhouse_go_server_format_demo_parquet"},
	{name: "Arrow", extension: ".arrow", table: "clickhouse_go_server_format_demo_arrow"},
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "server-side format demo:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	outputDir := env("CLICKHOUSE_FORMAT_OUTPUT_DIR", "server-formatted-output")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	auth := clickhouse.Auth{
		Database: env("CLICKHOUSE_DATABASE", "default"),
		Username: env("CLICKHOUSE_USER", "default"),
		Password: os.Getenv("CLICKHOUSE_PASSWORD"),
	}
	native, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{env("CLICKHOUSE_NATIVE_ADDR", "localhost:9000")},
		Auth:        auth,
		Protocol:    clickhouse.Native,
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return fmt.Errorf("open native connection: %w", err)
	}
	http, err := clickhouse.Open(&clickhouse.Options{
		Addr:     []string{env("CLICKHOUSE_HTTP_ADDR", "localhost:8123")},
		Auth:     auth,
		Protocol: clickhouse.HTTP,
	})
	if err != nil {
		return fmt.Errorf("open HTTP connection: %w", err)
	}
	if err := native.Ping(ctx); err != nil {
		return fmt.Errorf("native ping: %w", err)
	}
	if err := http.Ping(ctx); err != nil {
		return fmt.Errorf("HTTP ping: %w", err)
	}

	expectedRows, expectedChecksum, err := signature(ctx, native, sourceQuery)
	if err != nil {
		return fmt.Errorf("calculate source signature: %w", err)
	}

	for _, format := range formats {
		path := filepath.Join(outputDir, "result"+format.extension)
		bytesWritten, err := exportFile(ctx, native, format.name, path)
		if err != nil {
			return fmt.Errorf("export %s: %w", format.name, err)
		}
		if err := importFile(ctx, http, format, path); err != nil {
			return fmt.Errorf("import %s: %w", format.name, err)
		}
		rows, checksum, err := signature(ctx, native,
			fmt.Sprintf("SELECT id, name, score FROM %s", format.table))
		if err != nil {
			return fmt.Errorf("verify %s: %w", format.name, err)
		}
		if rows != expectedRows || checksum != expectedChecksum {
			return fmt.Errorf(
				"%s round trip changed data: got rows=%d checksum=%d, want rows=%d checksum=%d",
				format.name, rows, checksum, expectedRows, expectedChecksum)
		}
		absolutePath, err := filepath.Abs(path)
		if err != nil {
			return fmt.Errorf("resolve output path: %w", err)
		}
		fmt.Printf("%-7s %8d bytes  %s  ->  %s (%d rows, checksum %d)\n",
			format.name, bytesWritten, absolutePath, format.table, rows, checksum)
	}

	return nil
}

func exportFile(ctx context.Context, conn driver.Conn, format, path string) (int64, error) {
	// QueryFormat returns bytes produced by ClickHouse. This program neither
	// encodes rows nor imports a CSV, Parquet, or Arrow implementation.
	stream, err := conn.QueryFormat(ctx, format, sourceQuery)
	if err != nil {
		return 0, err
	}
	file, err := os.Create(path)
	if err != nil {
		_ = stream.Close()
		return 0, err
	}
	written, copyErr := io.Copy(file, stream)
	streamErr := stream.Close()
	fileErr := file.Close()
	if err := errors.Join(copyErr, streamErr, fileErr); err != nil {
		return written, err
	}
	return written, nil
}

func importFile(ctx context.Context, conn driver.Conn, format formatCase, path string) error {
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+format.table); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `CREATE TABLE `+format.table+` (
		id UInt64,
		name String,
		score Int64
	) ENGINE = Memory`); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// InsertFormat sends the file verbatim. ClickHouse's HTTP input-format
	// parser validates and decodes it on the server.
	return conn.InsertFormat(ctx, format.name, "INSERT INTO "+format.table, file)
}

func signature(ctx context.Context, conn driver.Conn, query string) (uint64, uint64, error) {
	var rows uint64
	var checksum uint64
	err := conn.QueryRow(ctx, `
		SELECT count(), groupBitXor(cityHash64(id, name, score))
		FROM (`+query+`)`).Scan(&rows, &checksum)
	return rows, checksum, err
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
