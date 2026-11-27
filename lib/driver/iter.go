package driver

import (
	"errors"
	"iter"
)

// StructIter returns an iterator that scans each row into T with ScanStruct.
// It works with native Rows over TCP and HTTP, not database/sql.Rows.
// It closes rows when iteration ends, including when the caller breaks early.
// A scan or terminal error is yielded once, with the zero value of T.
func StructIter[T any](rows Rows) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for rows.Next() {
			var value T
			if err := rows.ScanStruct(&value); err != nil {
				var zero T
				if closeErr := rows.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
				_ = yield(zero, err)
				return
			}
			if !yield(value, nil) {
				// The caller stopped iteration, so the protocol forbids yielding a close error.
				if closeErr := rows.Close(); closeErr != nil {
					return
				}
				return
			}
		}

		if err := rows.Err(); err != nil {
			// Next has already closed rows and recorded the terminal error.
			var zero T
			_ = yield(zero, err)
			return
		}

		if err := rows.Close(); err != nil {
			var zero T
			_ = yield(zero, err)
		}
	}
}
