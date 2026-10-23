package clickhouse

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// A server error in the middle of a rows stream must be returned when the rows are appended to a batch block by
// block, not lost: otherwise an incomplete stream is inserted as if it were complete.
func TestAppendRowsBlocksReturnsStreamError(t *testing.T) {
	serverErr := errors.New("server error in the middle of the stream")
	errs := make(chan error)
	stream := make(chan *proto.Block)
	go func() {
		// The stream stays open until the error is received, so rows.Next can only receive the error.
		errs <- serverErr
		close(errs)
		close(stream)
	}()

	r := &rows{block: proto.NewBlock(), errors: errs, stream: stream}
	b := &batch{}
	assert.ErrorIs(t, b.appendRowsBlocks(r), serverErr)
}
