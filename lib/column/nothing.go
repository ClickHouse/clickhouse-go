package column

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

type Nothing struct{}

func (Nothing) Type() Type                              { return "Nothing" }
func (Nothing) Rows() int                               { return 0 }
func (Nothing) RowValue(row int) interface{}            { return nil }
func (Nothing) ScanRow(dest interface{}, row int) error { return nil }
func (Nothing) Append(v interface{}) ([]uint8, error) {
	return nil, &StoreSpecialDataType{"Nothing"}
}
func (Nothing) AppendRow(v interface{}) error { return &StoreSpecialDataType{"Nothing"} }
func (Nothing) Decode(decoder *binary.Decoder, rows int) error {
	scratch := make([]byte, rows)
	if err := decoder.Raw(scratch); err != nil {
		return err
	}
	return nil
}
func (Nothing) Encode(encoder *binary.Encoder) error { return &StoreSpecialDataType{"Nothing"} }

var _ Interface = (*Nothing)(nil)
