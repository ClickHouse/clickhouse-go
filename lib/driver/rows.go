package driver

// CollectableRow is the subset of Rows methods that a RowToFunc is allowed to call.
type CollectableRow interface {
	Scan(dest ...any) error
	ScanStruct(dest any) error
	ColumnTypes() []ColumnType
	Totals(dest ...any) error
	Columns() []string
}

// RowToFunc is a function that scans or otherwise converts row to a T.
type RowToFunc[T any] func(row CollectableRow) (T, error)

// CollectRows iterates through rows, calling fn for each row, and collecting the results into a slice of T.
func CollectRows[T any](rows Rows, fn RowToFunc[T]) ([]T, error) {
	return AppendRows([]T{}, rows, fn)
}

// AppendRows iterates through rows, calling fn for each row, and appending the results into a slice of T.
func AppendRows[T any, S ~[]T](slice S, rows Rows, fn RowToFunc[T]) (S, error) {
	defer rows.Close()

	for rows.Next() {
		value, err := fn(rows)
		if err != nil {
			return nil, err
		}
		slice = append(slice, value)
	}

	if err := rows.Close(); err != nil {
		return nil, err
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return slice, nil
}
