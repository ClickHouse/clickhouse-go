package clickhouse_api

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type structColumnsRow struct {
	ID   uint64 `ch:"id"`
	Name string `ch:"name"`
}

func StructColumns() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	defer func() {
		conn.Exec(ctx, "DROP TABLE example_struct_columns")
	}()
	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS example_struct_columns`); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `
		CREATE TABLE example_struct_columns (
			id UInt64,
			name String,
			source String DEFAULT 'server default'
		) Engine = Memory
		`); err != nil {
		return err
	}

	columns, err := clickhouse.StructColumns(structColumnsRow{})
	if err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, fmt.Sprintf(
		"INSERT INTO example_struct_columns (%s)",
		strings.Join(columns, ", "),
	))
	if err != nil {
		return err
	}
	defer batch.Close()

	if err := batch.AppendStruct(&structColumnsRow{ID: 1, Name: "ClickHouse"}); err != nil {
		return err
	}
	return batch.Send()
}
