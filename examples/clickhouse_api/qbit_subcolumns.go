package clickhouse_api

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func QBitSubcolumns() error {
	conn, err := GetNativeConnection(clickhouse.Settings{
		// QBit is an experimental feature in ClickHouse
		"enable_qbit_type": 1,
	}, nil, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()

	conn.Exec(ctx, "DROP TABLE IF EXISTS example_subcolumns")

	// Create table with QBit column
	const ddl = `
		CREATE TABLE example_subcolumns (
			  id UInt8,
			  vec QBit(Float32, 4)
		) Engine MergeTree() ORDER BY id
		`

	if err := conn.Exec(ctx, ddl); err != nil {
		return err
	}
	fmt.Println("Table created with QBit column")

	// Insert vectors with special float values to demonstrate bit patterns
	// Float32 has 32 bits: 1 sign bit + 8 exponent bits + 23 mantissa bits
	// Bit numbering in QBit: .1 is MSB (sign bit), .32 is LSB
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO example_subcolumns")
	if err != nil {
		return err
	}

	// Insert vectors with positive and negative zeros to show sign bit difference
	vectors := []struct {
		id   uint8
		vec  []float32
		desc string
	}{
		{1, []float32{1.0, -1.0, 0.0, -0.0}, "Mixed signs"},
		{2, []float32{2.5, -2.5, 3.5, -3.5}, "Positive and negative"},
		{3, []float32{0.0, 0.0, 0.0, 0.0}, "All positive zeros"},
		{4, []float32{-0.0, -0.0, -0.0, -0.0}, "All negative zeros"},
	}

	for _, v := range vectors {
		if err := batch.Append(v.id, v.vec); err != nil {
			return err
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	fmt.Printf("Inserted %d vectors with special float values\n", batch.Rows())

	// Access subcolumns to examine bit patterns
	// .1 is the sign bit (MSB of Float32)
	// .2-.9 are exponent bits
	// .10-.32 are mantissa bits
	fmt.Println("\nAccessing QBit subcolumns:")
	fmt.Println("Note: vec.1 is the sign bit (1 = negative, 0 = positive)")
	fmt.Println()

	rows, err := conn.Query(ctx, `
		SELECT
			id,
			vec,
			bin(vec.1) as sign_bits,
			bin(vec.2) as exponent_bit1,
			bin(vec.9) as exponent_bit8
		FROM example_subcolumns
		ORDER BY id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id           uint8
			vec          []float32
			signBits     string
			exponentBit1 string
			exponentBit8 string
		)
		if err := rows.Scan(&id, &vec, &signBits, &exponentBit1, &exponentBit8); err != nil {
			return err
		}

		fmt.Printf("ID %d: %v\n", id, vec)
		fmt.Printf("  Sign bits (.1):      %s  (1=negative, 0=positive)\n", signBits)
		fmt.Printf("  Exponent bit 1 (.2): %s\n", exponentBit1)
		fmt.Printf("  Exponent bit 8 (.9): %s\n", exponentBit8)
		fmt.Println()
	}

	// Demonstrate sign bit difference between 0.0 and -0.0
	fmt.Println("Comparing 0.0 vs -0.0 (they differ only in sign bit):")

	var elem1, elem4 float32
	var elem1Sign, elem4Sign string
	if err := conn.QueryRow(ctx, `
		SELECT
			vec[1] as elem1,
			vec[4] as elem4,
			bin(vec.1)[1] as elem1_sign,
			bin(vec.1)[4] as elem4_sign
		FROM example_subcolumns
		WHERE id = 1
	`).Scan(&elem1, &elem4, &elem1Sign, &elem4Sign); err != nil {
		return err
	}

	fmt.Printf("Element 1: %v (sign bit: %s)\n", elem1, elem1Sign)
	fmt.Printf("Element 4: %v (sign bit: %s)\n", elem4, elem4Sign)
	fmt.Println()
	fmt.Println("Note: 0.0 and -0.0 are equal in value but have different sign bits!")

	// Show how to use subcolumns for approximate vector search
	fmt.Println("\nQBit subcolumns enable runtime precision tuning:")
	fmt.Println("- Read fewer bits (.1 to .8) for faster approximate search")
	fmt.Println("- Read more bits (.1 to .32) for higher accuracy")
	fmt.Println("- Use L2DistanceTransposed(vec1, vec2, precision) for optimized search")

	return nil
}
