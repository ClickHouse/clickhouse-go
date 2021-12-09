package column

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"fmt"
	chbin "github.com/ClickHouse/clickhouse-go/lib/binary"
	sdeci "github.com/shopspring/decimal"
	"math/big"
	"strings"
	"testing"
)

func TestDecimal_Write32(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []int32{
		0,
		1,
		-1,
		10,
		123,
		1234567,
		1234567890,
		-1234567890,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(5,3)",
			},
			nobits:    32,
			precision: 5,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int32(binary.LittleEndian.Uint32(buff.Bytes()))
		if value != attempt {
			t.Errorf("Expecting: %d; Got: %d", attempt, value)
		}
	}
}

func TestDecimal_WriteNullable32(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []sql.NullInt32{
		{Valid: true, Int32: 0},
		{Valid: true, Int32: 1},
		{Valid: true, Int32: -1},
		{Valid: true, Int32: 10},
		{Valid: true, Int32: 123},
		{Valid: true, Int32: 1234567},
		{Valid: true, Int32: 1234567890},
		{Valid: true, Int32: -1234567890},
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(5,3)",
			},
			nobits:    32,
			precision: 5,
			scale:     3,
		}

		attemptValue, err := attempt.Value()
		if err != nil {
			t.Fatal(err)
		}

		err = d.Write(encoder, attemptValue)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int32(binary.LittleEndian.Uint32(buff.Bytes()))
		if value != int32(attemptValue.(int64)) {
			t.Errorf("Expecting: %v; Got: %d", attemptValue, value)
		}
	}
}

func TestDecimal_Write32_WithInt64(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []int64{
		0,
		1,
		-1,
		10,
		123,
		1234567,
		1234567890,
		-1234567890,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(5,3)",
			},
			nobits:    32,
			precision: 5,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int32(binary.LittleEndian.Uint32(buff.Bytes()))
		if value != int32(attempt) {
			t.Errorf("Expecting: %d; Got: %d", attempt, value)
		}
	}
}

func TestDecimal_Write32_WithUint64(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []uint64{
		0,
		1,
		10,
		123,
		1234567,
		1234567890,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(5,3)",
			},
			nobits:    32,
			precision: 5,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int32(binary.LittleEndian.Uint32(buff.Bytes()))
		if value != int32(attempt) {
			t.Errorf("Expecting: %d; Got: %d", attempt, value)
		}
	}
}

func TestDecimal_Write64(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []int64{
		0,
		1,
		-1,
		10,
		123,
		1234567,
		1234567890,
		-1234567890,
		12345678901234,
		-12345678901234,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(10,3)",
			},
			nobits:    64,
			precision: 10,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int64(binary.LittleEndian.Uint64(buff.Bytes()))
		if value != attempt {
			t.Errorf("Expecting: %d; Got: %d", attempt, value)
		}
	}
}

func TestDecimal_WriteNullable64(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []sql.NullInt64{
		{Valid: true, Int64: 0},
		{Valid: true, Int64: 1},
		{Valid: true, Int64: -1},
		{Valid: true, Int64: 10},
		{Valid: true, Int64: 123},
		{Valid: true, Int64: 1234567},
		{Valid: true, Int64: 1234567890},
		{Valid: true, Int64: -1234567890},
		{Valid: true, Int64: 12345678901234},
		{Valid: true, Int64: -12345678901234},
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(10,3)",
			},
			nobits:    64,
			precision: 10,
			scale:     3,
		}

		attemptValue, err := attempt.Value()
		if err != nil {
			t.Fatal(err)
		}

		err = d.Write(encoder, attemptValue)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int64(binary.LittleEndian.Uint64(buff.Bytes()))
		if value != attemptValue.(int64) {
			t.Errorf("Expecting: %v; Got: %d", attemptValue, value)
		}
	}
}

func TestDecimal_Write64_WithUint64(t *testing.T) {
	t.Parallel()

	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)

	data := []uint64{
		0,
		1,
		10,
		123,
		1234567,
		1234567890,
		12345678901234,
	}

	for _, attempt := range data {
		buff.Reset()

		d := &Decimal{
			base: base{
				name:   "testcolumn",
				chType: "Decimal(10,3)",
			},
			nobits:    64,
			precision: 10,
			scale:     3,
		}

		err := d.Write(encoder, attempt)
		if err != nil {
			t.Fatal(err)
		}

		err = encoder.Flush()
		if err != nil {
			t.Fatal(err)
		}

		value := int64(binary.LittleEndian.Uint64(buff.Bytes()))
		if value != int64(attempt) {
			t.Errorf("Expecting: %d; Got: %d", attempt, value)
		}
	}
}

func TestDecimal_Write128_WithShopspringDecimal(t *testing.T) {
	t.Parallel()
	scales := []int{0, 2, 6}
	buff := &bytes.Buffer{}
	encoder := chbin.NewEncoder(buff)
	encoder.SelectParseDecimal(true)
	for _, scale := range scales {
		t.Run(fmt.Sprintf("scale:%d", scale), func(t *testing.T) {
			minD128, _ := new(big.Int).SetString("-"+strings.Repeat("9", 38-scale), 10)
			maxD128, _ := new(big.Int).SetString(strings.Repeat("9", 38-scale), 10)
			data := []sdeci.Decimal{
				sdeci.Zero,
				sdeci.NewFromInt32(-1),
				sdeci.NewFromInt(9223372036854775807),
				sdeci.NewFromInt(-9223372036854775808),
				sdeci.NewFromBigInt(minD128, 0),
				sdeci.NewFromBigInt(maxD128, 0),
			}

			for _, attempt := range data {
				buff.Reset()

				d := &Decimal{
					base: base{
						name:   "testcolumn",
						chType: fmt.Sprintf("Decimal(38,%d)", scale),
					},
					nobits:    128,
					precision: 38,
					scale:     scale,
				}

				err := d.Write(encoder, attempt)
				if err != nil {
					t.Fatal(err)
				}

				err = encoder.Flush()
				if err != nil {
					t.Fatal(err)
				}

				bi, err := decimal128ToBigInt(buff.Bytes())
				if err != nil {
					t.Error(err)
				}
				if value := sdeci.NewFromBigInt(bi, int32(-scale)); !attempt.Equal(value) {
					t.Errorf("Expecting: %s; Got: %s", attempt.String(), value.String())
				}
			}
		})
	}
}
