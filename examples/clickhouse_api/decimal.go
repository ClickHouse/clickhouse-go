
package clickhouse_api

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
)

func ReadWriteDecimal() error {
	conn, err := GetNativeConnection(nil, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err != nil {
		return err
	}
	conn.Exec(ctx, "DROP TABLE IF EXISTS example")

	if err = conn.Exec(ctx, `
		CREATE TABLE example (
			Col1 Decimal32(3), 
			Col2 Decimal(18,6), 
			Col3 Decimal(15,7), 
			Col4 Decimal128(8), 
			Col5 Decimal256(9)
		) Engine Memory
		`); err != nil {
		return err
	}

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example")
	if err != nil {
		return err
	}
	if err = batch.Append(
		decimal.New(25, 4),
		decimal.New(30, 5),
		decimal.New(35, 6),
		decimal.New(135, 7),
		decimal.New(256, 8),
	); err != nil {
		return err
	}

	if err = batch.Send(); err != nil {
		return err
	}

	var (
		col1 decimal.Decimal
		col2 decimal.Decimal
		col3 decimal.Decimal
		col4 decimal.Decimal
		col5 decimal.Decimal
	)

	if err = conn.QueryRow(ctx, "SELECT * FROM example").Scan(&col1, &col2, &col3, &col4, &col5); err != nil {
		return err
	}
	fmt.Printf("col1=%v, col2=%v, col3=%v, col4=%v, col5=%v\n", col1, col2, col3, col4, col5)
	return nil
}
