
package std

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
)

func AsyncInsert() error {
	conn, err := GetStdOpenDBConnection(clickhouse.Native, nil, nil, nil)
	if err != nil {
		return err
	}
	if _, err := conn.Exec(`DROP TABLE IF EXISTS example`); err != nil {
		return err
	}
	const ddl = `
		CREATE TABLE example (
			  Col1 UInt64
			, Col2 String
			, Col3 Array(UInt8)
			, Col4 DateTime
		) ENGINE = Memory
		`
	if _, err := conn.Exec(ddl); err != nil {
		return err
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithStdAsync(false))
	{
		for i := 0; i < 100; i++ {
			_, err := conn.ExecContext(ctx, `INSERT INTO example VALUES (
				?, ?, ?, now()
			)`, i, "Golang SQL database driver", []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
