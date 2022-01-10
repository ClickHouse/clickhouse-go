package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

type UnexpectedArguments = proto.UnexpectedArguments

type InvalidColumnIndex struct {
	op  string
	idx int
}

func (e *InvalidColumnIndex) Error() string {
	return fmt.Sprintf("clickhouse [%s]: invalid column index %d", e.op, e.idx)
}

type BindMixedNamedAndNumericParams struct{}

func (e *BindMixedNamedAndNumericParams) Error() string {
	return "clickhouse [bind]: mixed named and numeric parameters"
}

type UnexpectedPacket struct {
	op     string
	packet byte
}

func (e *UnexpectedPacket) Error() string {
	return fmt.Sprintf("clickhouse [%s]: unexpected packet %d", e.op, e.packet)
}
