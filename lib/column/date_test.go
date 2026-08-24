package column

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDateRejectsOutOfRangeValues(t *testing.T) {
	value := time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC)
	valuePtr := &value
	nullValue := sql.NullTime{Time: value, Valid: true}
	nullValuePtr := &nullValue
	stringValue := value.Format(defaultDateFormatNoZone)

	tests := []struct {
		name   string
		append func(*Date) error
	}{
		{
			name: "AppendRow time.Time",
			append: func(col *Date) error {
				return col.AppendRow(value)
			},
		},
		{
			name: "Append time.Time slice",
			append: func(col *Date) error {
				_, err := col.Append([]time.Time{value})
				return err
			},
		},
		{
			name: "Append time.Time pointer slice",
			append: func(col *Date) error {
				_, err := col.Append([]*time.Time{valuePtr})
				return err
			},
		},
		{
			name: "AppendRow sql.NullTime",
			append: func(col *Date) error {
				return col.AppendRow(nullValue)
			},
		},
		{
			name: "AppendRow sql.NullTime pointer",
			append: func(col *Date) error {
				return col.AppendRow(nullValuePtr)
			},
		},
		{
			name: "Append sql.NullTime slice",
			append: func(col *Date) error {
				_, err := col.Append([]sql.NullTime{nullValue})
				return err
			},
		},
		{
			name: "Append sql.NullTime pointer slice",
			append: func(col *Date) error {
				_, err := col.Append([]*sql.NullTime{nullValuePtr})
				return err
			},
		},
		{
			name: "AppendRow string",
			append: func(col *Date) error {
				return col.AppendRow(stringValue)
			},
		},
		{
			name: "Append string slice",
			append: func(col *Date) error {
				_, err := col.Append([]string{stringValue})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Date{}

			require.Error(t, tt.append(col))
			require.Zero(t, col.Rows())
		})
	}
}

func TestDateAcceptsSupportedRangeAndZeroValue(t *testing.T) {
	tests := []struct {
		name  string
		value time.Time
	}{
		{
			name:  "zero value",
			value: time.Time{},
		},
		{
			name:  "minimum date",
			value: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "maximum date with time of day",
			value: time.Date(2149, 6, 6, 23, 59, 59, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Date{}

			require.NoError(t, col.AppendRow(tt.value))
			require.Equal(t, 1, col.Rows())
		})
	}
}

func TestDateAcceptsMaximumValueThroughAllAppendPaths(t *testing.T) {
	value := time.Date(2149, 6, 6, 23, 59, 59, 0, time.UTC)
	valuePtr := &value
	nullValue := sql.NullTime{Time: value, Valid: true}
	nullValuePtr := &nullValue
	stringValue := "2149-06-06"
	stringValuePtr := &stringValue

	tests := []struct {
		name   string
		append func(*Date) error
	}{
		{
			name: "AppendRow time.Time",
			append: func(col *Date) error {
				return col.AppendRow(value)
			},
		},
		{
			name: "Append time.Time slice",
			append: func(col *Date) error {
				_, err := col.Append([]time.Time{value})
				return err
			},
		},
		{
			name: "Append time.Time pointer slice",
			append: func(col *Date) error {
				_, err := col.Append([]*time.Time{valuePtr})
				return err
			},
		},
		{
			name: "AppendRow sql.NullTime",
			append: func(col *Date) error {
				return col.AppendRow(nullValue)
			},
		},
		{
			name: "AppendRow sql.NullTime pointer",
			append: func(col *Date) error {
				return col.AppendRow(nullValuePtr)
			},
		},
		{
			name: "Append sql.NullTime slice",
			append: func(col *Date) error {
				_, err := col.Append([]sql.NullTime{nullValue})
				return err
			},
		},
		{
			name: "Append sql.NullTime pointer slice",
			append: func(col *Date) error {
				_, err := col.Append([]*sql.NullTime{nullValuePtr})
				return err
			},
		},
		{
			name: "AppendRow string",
			append: func(col *Date) error {
				return col.AppendRow(stringValue)
			},
		},
		{
			name: "Append string slice",
			append: func(col *Date) error {
				_, err := col.Append([]string{stringValue})
				return err
			},
		},
		{
			name: "Append string pointer slice",
			append: func(col *Date) error {
				_, err := col.Append([]*string{stringValuePtr})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Date{}

			require.NoError(t, tt.append(col))
			require.Equal(t, 1, col.Rows())
		})
	}
}

func TestDateAppendsNilNullTimePointerAsNull(t *testing.T) {
	col := &Date{}

	nulls, err := col.Append([]*sql.NullTime{nil})
	require.NoError(t, err)
	require.Equal(t, []uint8{1}, nulls)
	require.Equal(t, 1, col.Rows())
}

func TestDateBulkAppendIsAtomic(t *testing.T) {
	valid := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	invalid := time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
	validNull := sql.NullTime{Time: valid, Valid: true}
	invalidNull := sql.NullTime{Time: invalid, Valid: true}
	validString := "2020-01-01"
	invalidString := "1969-12-31"

	tests := []struct {
		name  string
		value any
	}{
		{
			name:  "time.Time slice",
			value: []time.Time{valid, invalid},
		},
		{
			name:  "time.Time pointer slice",
			value: []*time.Time{&valid, &invalid},
		},
		{
			name:  "sql.NullTime slice",
			value: []sql.NullTime{validNull, invalidNull},
		},
		{
			name:  "sql.NullTime pointer slice",
			value: []*sql.NullTime{&validNull, &invalidNull},
		},
		{
			name:  "string slice",
			value: []string{validString, invalidString},
		},
		{
			name:  "string pointer slice",
			value: []*string{&validString, &invalidString},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col := &Date{}
			require.NoError(t, col.AppendRow(valid))

			_, err := col.Append(tt.value)
			require.Error(t, err)
			require.Equal(t, 1, col.Rows())
		})
	}
}

func TestDateRejectsDatesAfterSupportedRange(t *testing.T) {
	col := &Date{}

	require.Error(t, col.AppendRow("2149-06-07"))
	require.Zero(t, col.Rows())
}
