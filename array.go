package clickhouse

import (
	"fmt"
	"time"

	"github.com/kshvakov/clickhouse/lib/types"
)

func Array(v interface{}) *types.Array {
	return types.NewArray(v)
}

func ArrayFixedString(len int, v interface{}) *types.Array {
	return types.NewArrayByType(fmt.Sprintf("FixedString(%d)", len), v)
}

func ArrayDate(v []time.Time) *types.Array {
	return types.NewArrayByType("Date", v)
}

func ArrayDateTime(v []time.Time) *types.Array {
	return types.NewArrayByType("DateTime", v)
}
