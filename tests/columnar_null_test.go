package tests

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestColumnarNullNumbers(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)

		const table = "test_columnar_null_numbers"
		const ddl = `
			CREATE TABLE test_columnar_null_numbers (
				  ID UInt8
				, Int16Value Nullable(Int16)
				, Int16Pointer Nullable(Int16)
				, Int32Value Nullable(Int32)
				, Int32Pointer Nullable(Int32)
				, Int64Value Nullable(Int64)
				, Int64Pointer Nullable(Int64)
				, Float64Value Nullable(Float64)
				, Float64Pointer Nullable(Float64)
			) Engine MergeTree() ORDER BY tuple()
		`
		t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+table) })
		require.NoError(t, conn.Exec(ctx, ddl))

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		require.NoError(t, err)

		int16PointerValid := sql.NullInt16{Int16: 17, Valid: true}
		int16PointerInvalid := sql.NullInt16{Int16: 98, Valid: false}
		int32PointerValid := sql.NullInt32{Int32: 33, Valid: true}
		int32PointerInvalid := sql.NullInt32{Int32: 98, Valid: false}
		int64PointerValid := sql.NullInt64{Int64: 65, Valid: true}
		int64PointerInvalid := sql.NullInt64{Int64: 98, Valid: false}
		float64PointerValid := sql.NullFloat64{Float64: 66.5, Valid: true}
		float64PointerInvalid := sql.NullFloat64{Float64: 98.5, Valid: false}

		require.NoError(t, batch.Column(0).Append([]uint8{1, 2, 3}))
		require.NoError(t, batch.Column(1).Append([]sql.NullInt16{
			{Int16: 16, Valid: true},
			{Int16: 99, Valid: false},
			{Int16: -16, Valid: true},
		}))
		require.NoError(t, batch.Column(2).Append([]*sql.NullInt16{
			&int16PointerValid,
			&int16PointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(3).Append([]sql.NullInt32{
			{Int32: 32, Valid: true},
			{Int32: 99, Valid: false},
			{Int32: -32, Valid: true},
		}))
		require.NoError(t, batch.Column(4).Append([]*sql.NullInt32{
			&int32PointerValid,
			&int32PointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(5).Append([]sql.NullInt64{
			{Int64: 64, Valid: true},
			{Int64: 99, Valid: false},
			{Int64: -64, Valid: true},
		}))
		require.NoError(t, batch.Column(6).Append([]*sql.NullInt64{
			&int64PointerValid,
			&int64PointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(7).Append([]sql.NullFloat64{
			{Float64: 64.5, Valid: true},
			{Float64: 99.5, Valid: false},
			{Float64: -64.5, Valid: true},
		}))
		require.NoError(t, batch.Column(8).Append([]*sql.NullFloat64{
			&float64PointerValid,
			&float64PointerInvalid,
			nil,
		}))
		require.Equal(t, 3, batch.Rows())
		require.NoError(t, batch.Send())

		type row struct {
			ID             uint8
			Int16Value     sql.NullInt16
			Int16Pointer   sql.NullInt16
			Int32Value     sql.NullInt32
			Int32Pointer   sql.NullInt32
			Int64Value     sql.NullInt64
			Int64Pointer   sql.NullInt64
			Float64Value   sql.NullFloat64
			Float64Pointer sql.NullFloat64
		}
		expected := []row{
			{
				ID:             1,
				Int16Value:     sql.NullInt16{Int16: 16, Valid: true},
				Int16Pointer:   sql.NullInt16{Int16: 17, Valid: true},
				Int32Value:     sql.NullInt32{Int32: 32, Valid: true},
				Int32Pointer:   sql.NullInt32{Int32: 33, Valid: true},
				Int64Value:     sql.NullInt64{Int64: 64, Valid: true},
				Int64Pointer:   sql.NullInt64{Int64: 65, Valid: true},
				Float64Value:   sql.NullFloat64{Float64: 64.5, Valid: true},
				Float64Pointer: sql.NullFloat64{Float64: 66.5, Valid: true},
			},
			{ID: 2},
			{
				ID:           3,
				Int16Value:   sql.NullInt16{Int16: -16, Valid: true},
				Int32Value:   sql.NullInt32{Int32: -32, Valid: true},
				Int64Value:   sql.NullInt64{Int64: -64, Valid: true},
				Float64Value: sql.NullFloat64{Float64: -64.5, Valid: true},
			},
		}

		rows, err := conn.Query(ctx, "SELECT * FROM "+table+" ORDER BY ID")
		require.NoError(t, err)
		actual := make([]row, 0, len(expected))
		for rows.Next() {
			var value row
			require.NoError(t, rows.Scan(
				&value.ID,
				&value.Int16Value,
				&value.Int16Pointer,
				&value.Int32Value,
				&value.Int32Pointer,
				&value.Int64Value,
				&value.Int64Pointer,
				&value.Float64Value,
				&value.Float64Pointer,
			))
			actual = append(actual, value)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		assert.Equal(t, expected, actual)
	})
}

func TestColumnarNullTimes(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)
		if !CheckMinServerServerVersion(conn, 21, 9, 0) {
			t.Skip(fmt.Errorf("unsupported clickhouse version"))
			return
		}

		const table = "test_columnar_null_times"
		const ddl = `
			CREATE TABLE test_columnar_null_times (
				  ID UInt8
				, DateValue Nullable(Date)
				, DatePointer Nullable(Date)
				, Date32Value Nullable(Date32)
				, Date32Pointer Nullable(Date32)
				, DateTimeValue Nullable(DateTime)
				, DateTimePointer Nullable(DateTime)
				, DateTime64Value Nullable(DateTime64(3))
				, DateTime64Pointer Nullable(DateTime64(3))
			) Engine MergeTree() ORDER BY tuple()
		`
		t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+table) })
		require.NoError(t, conn.Exec(ctx, ddl))

		date := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
		dateAlternative := time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC)
		datetime := time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC)
		datetimeAlternative := time.Date(2024, 2, 3, 4, 5, 6, 456000000, time.UTC)
		datePointerValid := sql.NullTime{Time: dateAlternative, Valid: true}
		datePointerInvalid := sql.NullTime{Time: datetimeAlternative, Valid: false}
		datetimePointerValid := sql.NullTime{Time: datetimeAlternative, Valid: true}
		datetimePointerInvalid := sql.NullTime{Time: dateAlternative, Valid: false}

		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		require.NoError(t, err)
		require.NoError(t, batch.Column(0).Append([]uint8{1, 2, 3}))
		require.NoError(t, batch.Column(1).Append([]sql.NullTime{
			{Time: date, Valid: true},
			{Time: dateAlternative, Valid: false},
			{Time: dateAlternative, Valid: true},
		}))
		require.NoError(t, batch.Column(2).Append([]*sql.NullTime{
			&datePointerValid,
			&datePointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(3).Append([]sql.NullTime{
			{Time: date, Valid: true},
			{Time: dateAlternative, Valid: false},
			{Time: dateAlternative, Valid: true},
		}))
		require.NoError(t, batch.Column(4).Append([]*sql.NullTime{
			&datePointerValid,
			&datePointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(5).Append([]sql.NullTime{
			{Time: datetime, Valid: true},
			{Time: datetimeAlternative, Valid: false},
			{Time: datetimeAlternative, Valid: true},
		}))
		require.NoError(t, batch.Column(6).Append([]*sql.NullTime{
			&datetimePointerValid,
			&datetimePointerInvalid,
			nil,
		}))
		require.NoError(t, batch.Column(7).Append([]sql.NullTime{
			{Time: datetime, Valid: true},
			{Time: datetimeAlternative, Valid: false},
			{Time: datetimeAlternative, Valid: true},
		}))
		require.NoError(t, batch.Column(8).Append([]*sql.NullTime{
			&datetimePointerValid,
			&datetimePointerInvalid,
			nil,
		}))
		require.Equal(t, 3, batch.Rows())
		require.NoError(t, batch.Send())

		type row struct {
			ID                uint8
			DateValue         sql.NullTime
			DatePointer       sql.NullTime
			Date32Value       sql.NullTime
			Date32Pointer     sql.NullTime
			DateTimeValue     sql.NullTime
			DateTimePointer   sql.NullTime
			DateTime64Value   sql.NullTime
			DateTime64Pointer sql.NullTime
		}
		rows, err := conn.Query(ctx, "SELECT * FROM "+table+" ORDER BY ID")
		require.NoError(t, err)
		actual := make([]row, 0, 3)
		for rows.Next() {
			var value row
			require.NoError(t, rows.Scan(
				&value.ID,
				&value.DateValue,
				&value.DatePointer,
				&value.Date32Value,
				&value.Date32Pointer,
				&value.DateTimeValue,
				&value.DateTimePointer,
				&value.DateTime64Value,
				&value.DateTime64Pointer,
			))
			actual = append(actual, value)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		require.Len(t, actual, 3)
		assert.Equal(t, uint8(1), actual[0].ID)
		assertNullTime(t, date, actual[0].DateValue)
		assertNullTime(t, dateAlternative, actual[0].DatePointer)
		assertNullTime(t, date, actual[0].Date32Value)
		assertNullTime(t, dateAlternative, actual[0].Date32Pointer)
		assertNullTime(t, datetime, actual[0].DateTimeValue)
		assertNullTime(t, datetimeAlternative, actual[0].DateTimePointer)
		assertNullTime(t, datetime, actual[0].DateTime64Value)
		assertNullTime(t, datetimeAlternative, actual[0].DateTime64Pointer)

		assert.Equal(t, uint8(2), actual[1].ID)
		assert.False(t, actual[1].DateValue.Valid)
		assert.False(t, actual[1].DatePointer.Valid)
		assert.False(t, actual[1].Date32Value.Valid)
		assert.False(t, actual[1].Date32Pointer.Valid)
		assert.False(t, actual[1].DateTimeValue.Valid)
		assert.False(t, actual[1].DateTimePointer.Valid)
		assert.False(t, actual[1].DateTime64Value.Valid)
		assert.False(t, actual[1].DateTime64Pointer.Valid)

		assert.Equal(t, uint8(3), actual[2].ID)
		assertNullTime(t, dateAlternative, actual[2].DateValue)
		assert.False(t, actual[2].DatePointer.Valid)
		assertNullTime(t, dateAlternative, actual[2].Date32Value)
		assert.False(t, actual[2].Date32Pointer.Valid)
		assertNullTime(t, datetimeAlternative, actual[2].DateTimeValue)
		assert.False(t, actual[2].DateTimePointer.Valid)
		assertNullTime(t, datetimeAlternative, actual[2].DateTime64Value)
		assert.False(t, actual[2].DateTime64Pointer.Valid)
	})
}

func TestColumnarNullString(t *testing.T) {
	TestProtocols(t, func(t *testing.T, protocol clickhouse.Protocol) {
		conn, err := GetNativeConnection(t, protocol, nil, nil, &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		})
		ctx := context.Background()
		require.NoError(t, err)

		const table = "test_columnar_null_string"
		const ddl = `
			CREATE TABLE test_columnar_null_string (
				  ID UInt8
				, Value Nullable(String)
				, Pointer Nullable(String)
			) Engine MergeTree() ORDER BY tuple()
		`
		t.Cleanup(func() { _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+table) })
		require.NoError(t, conn.Exec(ctx, ddl))

		pointerValid := sql.NullString{String: "pointer", Valid: true}
		pointerInvalid := sql.NullString{String: "hidden", Valid: false}
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table)
		require.NoError(t, err)
		require.NoError(t, batch.Column(0).Append([]uint8{1, 2, 3}))
		require.NoError(t, batch.Column(1).Append([]sql.NullString{
			{String: "value", Valid: true},
			{String: "hidden", Valid: false},
			{String: "", Valid: true},
		}))
		require.NoError(t, batch.Column(2).Append([]*sql.NullString{
			&pointerValid,
			&pointerInvalid,
			nil,
		}))
		require.Equal(t, 3, batch.Rows())
		require.NoError(t, batch.Send())

		rows, err := conn.Query(ctx, "SELECT ID, Value, Pointer FROM "+table+" ORDER BY ID")
		require.NoError(t, err)
		type row struct {
			ID      uint8
			Value   sql.NullString
			Pointer sql.NullString
		}
		var actual []row
		for rows.Next() {
			var value row
			require.NoError(t, rows.Scan(&value.ID, &value.Value, &value.Pointer))
			actual = append(actual, value)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		assert.Equal(t, []row{
			{ID: 1, Value: sql.NullString{String: "value", Valid: true}, Pointer: sql.NullString{String: "pointer", Valid: true}},
			{ID: 2},
			{ID: 3, Value: sql.NullString{Valid: true}},
		}, actual)
	})
}

func assertNullTime(t *testing.T, expected time.Time, actual sql.NullTime) {
	t.Helper()
	if !assert.True(t, actual.Valid) {
		return
	}
	assert.Equal(t, expected.UnixNano(), actual.Time.UnixNano())
}
