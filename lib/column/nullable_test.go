package column

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestNullableScanRowClearsPointerToPointer(t *testing.T) {
	col := &Nullable{enable: true}
	col.nulls.Append(1)

	value := decimal.RequireFromString("123.1")
	dest := &value

	require.NoError(t, col.ScanRow(&dest, 0))
	require.Nil(t, dest)
}
