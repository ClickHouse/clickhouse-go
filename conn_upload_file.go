package clickhouse

import (
	"context"
	"fmt"
	"io"
)

func (c *connect) uploadFile(ctx context.Context, reader io.Reader, query string) error {
	return fmt.Errorf("UploadFile is not implemented for Native connector")
}
