package clickhouse

type Progress struct {
	Rows      uint64
	Bytes     uint64
	TotalRows uint64
}

func (p *Progress) update(ch *clickhouse) error {
	if rows, err := ch.decoder.Uvarint(); err != nil {
		return err
	} else {
		p.Rows = p.Rows + rows
	}
	if bytes, err := ch.decoder.Uvarint(); err != nil {
		return err
	} else {
		p.Bytes = p.Bytes + bytes
	}
	if totalRows, err := ch.decoder.Uvarint(); err != nil {
		return err
	} else {
		p.TotalRows = p.TotalRows + totalRows
	}
	return nil
}
