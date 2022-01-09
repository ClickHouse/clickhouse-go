package clickhouse

import (
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/lib/proto"
)

type onProcess struct {
	data     func(*proto.Block)
	progress func(*Progress)
}

func (c *connect) firstBlock(on *onProcess) (*proto.Block, error) {
	for {
		packet, err := c.decoder.ReadByte()
		if err != nil {
			return nil, err
		}
		switch packet {
		case proto.ServerData:
			return c.readData(true)
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil, io.EOF
		default:
			if err := c.handle(packet, on); err != nil {
				return nil, err
			}
		}
	}
}

func (c *connect) process(on *onProcess) error {
	for {
		packet, err := c.decoder.ReadByte()
		if err != nil {
			return err
		}
		switch packet {
		case proto.ServerEndOfStream:
			c.debugf("[end of stream]")
			return nil
		}
		if err := c.handle(packet, on); err != nil {
			return err
		}
	}
}

func (c *connect) handle(packet byte, on *onProcess) error {
	switch packet {
	case proto.ServerData, proto.ServerTotals, proto.ServerExtremes:
		block, err := c.readData(true)
		if err != nil {
			return err
		}
		if on.data != nil {
			on.data(block)
		}
	case proto.ServerException:
		return c.exception()
	case proto.ServerProfileInfo:
		var info proto.ProfileInfo
		if err := info.Decode(c.decoder, c.revision); err != nil {
			return err
		}
		c.debugf("[profile info] %s", &info)
	case proto.ServerTableColumns:
		var info proto.TableColumns
		if err := info.Decode(c.decoder, c.revision); err != nil {
			return err
		}
		c.debugf("[table columns]")
	case proto.ServerProfileEvents:
		if err := c.profileEvents(); err != nil {
			return err
		}
	case proto.ServerLog:
		if err := c.logs(); err != nil {
			return err
		}
	case proto.ServerProgress:
		progress, err := c.progress()
		if err != nil {
			return err
		}
		c.debugf("[progress] %s", progress)
		if on.progress != nil {
			on.progress(progress)
		}
	default:
		return fmt.Errorf("unexpected packet %d", packet)
	}
	return nil
}
