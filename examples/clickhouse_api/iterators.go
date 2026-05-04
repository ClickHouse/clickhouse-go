package clickhouse_api

import (
	"context"
	"fmt"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func Iterators() (err error) {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}

	ctx := context.Background()
	defer func() {
		if dropErr := conn.Exec(ctx, "DROP TABLE example_iterators"); dropErr != nil && err == nil {
			err = fmt.Errorf("drop example_iterators: %w", dropErr)
		}
	}()

	if err := conn.Exec(ctx, `DROP TABLE IF EXISTS example_iterators`); err != nil {
		return err
	}
	if err := conn.Exec(ctx, `
		CREATE TABLE example_iterators (
			Col1 UInt8,
			Col2 String
		) ENGINE = Memory
	`); err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example_iterators")
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		if err := batch.Append(uint8(i), fmt.Sprintf("value_%d", i)); err != nil {
			return err
		}
	}
	if err := batch.Send(); err != nil {
		return err
	}

	type result struct {
		Col1 uint8
		Col2 string
	}

	rows, err := conn.Query(ctx, "SELECT Col1, Col2 FROM example_iterators ORDER BY Col1")
	if err != nil {
		return err
	}
	for value, err := range chdriver.StructIter[result](rows) {
		if err != nil {
			return err
		}
		fmt.Printf("struct row: col1=%d, col2=%s\n", value.Col1, value.Col2)
	}

	return nil
}
