package column

import (
	"fmt"
	"time"
)

const secInDay = 24 * 60 * 60

func dateOverflow(min, max, v time.Time, format string) error {
	if v.Before(min) || v.After(max) {
		return &DateOverflowError{
			Min:    min,
			Max:    max,
			Value:  v,
			Format: format,
		}
	}
	return nil
}

type DateOverflowError struct {
	Min, Max time.Time
	Value    time.Time
	Format   string
}

func (e *DateOverflowError) Error() string {
	return fmt.Sprintf("clickhouse: dateTime overflow. must be between %s and %s", e.Min.Format(e.Format), e.Max.Format(e.Format))
}
