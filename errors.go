package clickhouse

import "fmt"

type UnexpectedArguments struct{ got, want int }

func (e *UnexpectedArguments) Error() string {
	return fmt.Sprintf("clickhouse: expected %d arguments, got %d", e.want, e.got)
}
