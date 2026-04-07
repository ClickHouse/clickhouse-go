package driver

import "iter"

func StructIter[T any](rows Rows) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		defer rows.Close()

		for rows.Next() {
			var value T
			if err := rows.ScanStruct(&value); err != nil {
				var zero T
				_ = yield(zero, err)
				return
			}
			if !yield(value, nil) {
				return
			}
		}

		if err := rows.Err(); err != nil {
			var zero T
			_ = yield(zero, err)
		}
	}
}
