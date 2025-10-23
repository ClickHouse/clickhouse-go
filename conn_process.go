package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

type onProcess struct {
	data          func(*proto.Block)
	logs          func([]Log)
	progress      func(*Progress)
	profileInfo   func(*ProfileInfo)
	profileEvents func([]ProfileEvent)
}

func (c *connect) firstBlock(ctx context.Context, on *onProcess) (*proto.Block, error) {
	// if context is already timedout/cancelled — we're done
	select {
	case <-ctx.Done():
		c.cancel()
		return nil, ctx.Err()
	default:
	}

	// do reads in background
	resultCh := make(chan *proto.Block, 1)
	errCh := make(chan error, 1)

	go func() {
		block, err := c.firstBlockImpl(ctx, on)
		if err != nil {
			errCh <- err
			return
		}
		resultCh <- block
	}()

	// select on context or read channels (results/errors)
	select {
	case <-ctx.Done():
		c.cancel()
		return nil, ctx.Err()

	case err := <-errCh:
		return nil, err

	case block := <-resultCh:
		return block, nil
	}
}

func (c *connect) firstBlockImpl(ctx context.Context, on *onProcess) (*proto.Block, error) {
	c.readerMutex.Lock()
	defer c.readerMutex.Unlock()

	c.startReadWriteTimeout(ctx)
	defer c.clearReadWriteTimeout(ctx)

	for {
		if c.reader == nil {
			return nil, errors.New("unexpected state: c.reader is nil")
		}

		packet, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		switch packet {
		case proto.ServerData:
			return c.readData(ctx, packet, true)

		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil, io.EOF

		default:
			if err := c.handle(ctx, packet, on); err != nil {
				// handling error, return
				return nil, err
			}

			// handled okay, read next byte
		}
	}
}

func (c *connect) process(ctx context.Context, on *onProcess) error {
	// if context is already timedout/cancelled — we're done
	select {
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()
	default:
	}

	// do reads in background
	errCh := make(chan error, 1)
	doneCh := make(chan bool, 1)

	go func() {
		err := c.processImpl(ctx, on)
		if err != nil {
			errCh <- err
			return
		}

		doneCh <- true
	}()

	// select on context or read channel (errors)
	select {
	case <-ctx.Done():
		c.cancel()
		return ctx.Err()

	case err := <-errCh:
		return err

	case <-doneCh:
		return nil
	}
}

func (c *connect) processImpl(ctx context.Context, on *onProcess) error {
	c.readerMutex.Lock()
	defer c.readerMutex.Unlock()

	c.startReadWriteTimeout(ctx)
	defer c.clearReadWriteTimeout(ctx)

	for {
		if c.reader == nil {
			return errors.New("unexpected state: c.reader is nil")
		}

		packet, err := c.reader.ReadByte()
		if err != nil {
			return err
		}

		switch packet {
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil
		}

		if err := c.handle(ctx, packet, on); err != nil {
			// handling error, return
			return err
		}

		// handled okay, read next byte
	}
}

func (c *connect) handle(ctx context.Context, packet byte, on *onProcess) error {
	switch packet {
	case proto.ServerData, proto.ServerTotals, proto.ServerExtremes:
		block, err := c.readData(ctx, packet, true)
		if err != nil {
			return err
		}
		if block.Rows() != 0 && on.data != nil {
			on.data(block)
		}
	case proto.ServerException:
		return c.exception()
	case proto.ServerProfileInfo:
		var info proto.ProfileInfo
		if err := info.Decode(c.reader, c.revision); err != nil {
			return err
		}
		c.debugf("[profile info] %s", &info)
		on.profileInfo(&info)
	case proto.ServerTableColumns:
		var info proto.TableColumns
		if err := info.Decode(c.reader, c.revision); err != nil {
			return err
		}
		c.debugf("[table columns]")
	case proto.ServerProfileEvents:
		scanEvents := on.profileEvents != nil
		events, err := c.profileEvents(ctx, scanEvents)
		if err != nil {
			return err
		}
		if scanEvents {
			on.profileEvents(events)
		}
	case proto.ServerLog:
		logs, err := c.logs(ctx)
		if err != nil {
			return err
		}
		on.logs(logs)
	case proto.ServerProgress:
		progress, err := c.progress()
		if err != nil {
			return err
		}
		c.debugf("[progress] %s", progress)
		on.progress(progress)
	default:
		return &OpError{
			Op:  "process",
			Err: fmt.Errorf("unexpected packet %d", packet),
		}
	}
	return nil
}

func (c *connect) cancel() error {
	c.debugf("[cancel]")
	c.buffer.PutUVarInt(proto.ClientCancel)
	wErr := c.flush()
	// don't reuse a cancelled query as we don't drain the connection
	if cErr := c.close(); cErr != nil {
		return cErr
	}
	return wErr
}
