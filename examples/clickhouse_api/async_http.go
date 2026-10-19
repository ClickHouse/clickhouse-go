package clickhouse_api

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_tests "github.com/ClickHouse/clickhouse-go/v2/tests"
)

func AsyncInsertHTTP() error {
	conn, err := GetHTTPConnection("session-async-insert-http-example", nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 21, 12, 0) {
		return nil
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE example")
	}()
	conn.Exec(ctx, `DROP TABLE IF EXISTS example`)
	const ddl = `
		CREATE TABLE example (
			  Col1 UInt64
			, Col2 String
			, Col3 Array(UInt8)
			, Col4 DateTime
		) ENGINE = Memory
	`

	if err := conn.Exec(ctx, ddl); err != nil {
		return err
	}

	ctx = clickhouse.Context(ctx, clickhouse.WithAsync(false))
	{
		for i := 0; i < 100; i++ {
			err := conn.Exec(ctx, `INSERT INTO example VALUES (
				?, ?, ?, now()
			)`, i, "Golang SQL database driver", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func AsyncInsertHTTP_WithBatch() error {
	conn, err := GetHTTPConnection("session-async-insert-http-batch-example", nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if !clickhouse_tests.CheckMinServerServerVersion(conn, 21, 12, 0) {
		return nil
	}
	defer func() {
		conn.Exec(ctx, "DROP TABLE example")
	}()
	conn.Exec(ctx, `DROP TABLE IF EXISTS example`)
	const ddl = `
		CREATE TABLE example (
			  Col1 UInt64
			, Col2 String
			, Col3 Array(UInt8)
			, Col4 DateTime
		) ENGINE = Memory
	`

	if err := conn.Exec(ctx, ddl); err != nil {
		return err
	}

	// The batch uses only the context from PrepareBatch, so WithAsync must be on it.
	ctx = clickhouse.Context(ctx, clickhouse.WithAsync(true))
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
	if err != nil {
		return err
	}
	defer batch.Close()

	for i := 0; i < 100; i++ {
		err := batch.Append(uint64(i), "Golang SQL database driver", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}, time.Now())
		if err != nil {
			return err
		}
	}
	return batch.Send()
}
