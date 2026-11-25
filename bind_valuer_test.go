package clickhouse

import (
	std_driver "database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type valueReceiverValuer struct{ s string }

func (v valueReceiverValuer) Value() (std_driver.Value, error) { return v.s, nil }

type pointerReceiverValuer struct{}

func (v *pointerReceiverValuer) Value() (std_driver.Value, error) {
	if v == nil {
		return "nil receiver", nil
	}
	return "set", nil
}

func TestBindTypedNilValuer(t *testing.T) {
	var nilValue *valueReceiverValuer
	var nilPointer *pointerReceiverValuer
	for _, tc := range []struct {
		name  string
		query string
		args  []any
		want  string
	}{
		{"positional", "SELECT ?", []any{nilValue}, "SELECT NULL"},
		{"numeric", "SELECT $1", []any{nilValue}, "SELECT NULL"},
		{"named", "SELECT @v", []any{Named("v", nilValue)}, "SELECT NULL"},
		// Like database/sql, a pointer-receiver Value method still runs on nil.
		{"pointer_receiver", "SELECT ?", []any{nilPointer}, "SELECT 'nil receiver'"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			var err error
			require.NotPanics(t, func() { got, err = bind(time.UTC, tc.query, tc.args...) })
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
