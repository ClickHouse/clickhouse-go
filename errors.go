package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
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

type UnexpectedScanDestination struct {
	op       string
	got      int
	expected int
}

func (e *UnexpectedScanDestination) Error() string {
	return fmt.Sprintf("clickhouse [%s]: expected %d destination arguments in Scan, not %d", e.op, e.expected, e.got)
}

type BatchAlreadySent struct{}

func (e *BatchAlreadySent) Error() string {
	return "clickhouse: batch has already been sent"
}

type AcquireConnTimeout struct {
}

func (e *AcquireConnTimeout) Error() string {
	return "acquire conn timeout"
}
