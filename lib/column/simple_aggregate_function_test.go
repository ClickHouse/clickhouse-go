package column

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSimpleAggregateFunctionParseMalformed(t *testing.T) {
	sc := &ServerContext{Timezone: time.UTC}
	tests := []struct {
		name   string
		chType Type
	}{
		{name: "no parens", chType: "SimpleAggregateFunction"},
		{name: "empty parens", chType: "SimpleAggregateFunction()"},
		{name: "function only", chType: "SimpleAggregateFunction(anyLast)"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				col, err := tt.chType.Column("col", sc)
				require.Error(t, err)
				assert.Nil(t, col)
			})
		})
	}
}

func TestSimpleAggregateFunctionParseValid(t *testing.T) {
	sc := &ServerContext{Timezone: time.UTC}
	col, err := Type("SimpleAggregateFunction(anyLast, UInt64)").Column("col", sc)
	require.NoError(t, err)
	require.IsType(t, &SimpleAggregateFunction{}, col)
	saf := col.(*SimpleAggregateFunction)
	require.NotNil(t, saf.base)
	assert.Equal(t, Type("UInt64"), saf.base.Type())
}
