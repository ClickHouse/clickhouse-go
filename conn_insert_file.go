package clickhouse

import (
	"context"
	"fmt"
)

func (c *connect) insertFile(ctx context.Context, filePath string, query string) error {
	return fmt.Errorf("InsertFile is not implemented for Native connector")
}
