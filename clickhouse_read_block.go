package clickhouse

import (
	"github.com/kshvakov/clickhouse/lib/data"
)

func (ch *clickhouse) readBlock() (*data.Block, error) {
	if _, err := ch.decoder.String(); err != nil { // temporary table
		return nil, err
	}

	ch.decoder.SelectCompress(ch.compress)
	var block data.Block
	option := data.BlockRWOption{
		Timezone:    ch.ServerInfo.Timezone,
		DecimalMode: ch.decimalMode,
	}
	if err := block.Read(&option, ch.decoder); err != nil {
		return nil, err
	}
	ch.decoder.SelectCompress(false)
	return &block, nil
}
