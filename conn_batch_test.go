package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// https://github.com/ClickHouse/clickhouse-go/issues/2016
func TestBatchAppendRowsSourceStreamError(t *testing.T) {
	block := &proto.Block{ServerContext: &column.ServerContext{}}
	require.NoError(t, block.AddColumn("x", "Int64"))
	require.NoError(t, block.Append(int64(1)))

	streamErr := errors.New("mid-stream failure")
	stream := make(chan *proto.Block)
	errs := make(chan error, 1)
	// Same ordering as connect.query's processing goroutine on failure.
	errs <- streamErr
	close(stream)
	close(errs)

	src := &rows{block: block, stream: stream, errors: errs}

	var releasedWith error
	releaseCalls := 0
	b := &batch{
		ctx:   context.Background(),
		conn:  &connect{logger: newNoopLogger()},
		block: block,
		connRelease: func(_ *connect, err error) {
			releaseCalls++
			releasedWith = err
		},
	}

	err := b.Append(src)
	require.ErrorIs(t, err, streamErr)

	assert.Equal(t, 1, releaseCalls)
	assert.ErrorIs(t, releasedWith, streamErr, "connection must be released with the error so it is not reused")

	// Send marks the batch as sent, so it must run last.
	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"Append", func() error { return b.Append(int64(2)) }},
		{"Flush", b.Flush},
		{"Send", b.Send},
	} {
		err := tc.call()
		assert.ErrorIs(t, err, ErrBatchInvalid, tc.name)
		assert.ErrorIs(t, err, streamErr, tc.name)
	}
}
