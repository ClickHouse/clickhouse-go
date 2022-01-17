package column

import (
	"fmt"
	"math"
	"reflect"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/binary"
)

const indexTypeMask = 0b11111111

const (
	keyUInt8  = 0
	keyUInt16 = 1
	keyUInt32 = 2
	keyUInt64 = 3
)
const (
	/// Need to read dictionary if it wasn't.
	needGlobalDictionaryBit = 1 << 8
	/// Need to read additional keys. Additional keys are stored before indexes as value N and N keys after them.
	hasAdditionalKeysBit = 1 << 9
	/// Need to update dictionary. It means that previous granule has different dictionary.
	needUpdateDictionary = 1 << 10

	updateAll = hasAdditionalKeysBit | needUpdateDictionary
)

const sharedDictionariesWithAdditionalKeys = 1

// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnLowCardinality.cpp
// https://github.com/ClickHouse/clickhouse-cpp/blob/master/clickhouse/columns/lowcardinality.cpp
type LowCardinality struct {
	key    byte
	index  Interface
	chType Type

	keys8  UInt8
	keys16 UInt16
	keys32 UInt32
	keys64 UInt64

	tmpIdx map[interface{}]int
	tmpKey []int
}

func (col *LowCardinality) parse(t Type) (_ *LowCardinality, err error) {
	col.chType = t
	col.tmpIdx = make(map[interface{}]int)
	if col.index, err = Type(t.params()).Column(); err != nil {
		return nil, err
	}
	return col, nil
}

func (col *LowCardinality) Type() Type {
	return col.chType
}

func (col *LowCardinality) ScanType() reflect.Type {
	return col.index.ScanType()
}

func (col *LowCardinality) Rows() int {
	if len(col.tmpKey) != 0 {
		return len(col.tmpKey)
	}
	return col.keys().Rows()
}

func (col *LowCardinality) Row(i int, ptr bool) interface{} {
	return col.index.Row(col.indexRowNum(i), ptr)
}

func (col *LowCardinality) ScanRow(dest interface{}, row int) error {
	return col.index.ScanRow(dest, col.indexRowNum(row))
}

func (col *LowCardinality) Append(v interface{}) (nulls []uint8, err error) {
	value := reflect.Indirect(reflect.ValueOf(v))
	if value.Kind() != reflect.Slice {
		return nil, &ColumnConverterErr{
			op:   "Append",
			to:   string(col.chType),
			from: fmt.Sprintf("%T", v),
		}
	}
	for i := 0; i < value.Len(); i++ {
		if err := col.AppendRow(value.Index(i).Interface()); err != nil {
			return nil, err
		}
	}
	return
}

func (col *LowCardinality) AppendRow(v interface{}) error {
	switch x := v.(type) {
	case time.Time:
		v = x.Truncate(time.Second)
	}
	if _, found := col.tmpIdx[v]; !found {
		if v == nil {
			return fmt.Errorf("clickhouse: LowCardinality does not support NULL values")
		}
		if err := col.index.AppendRow(v); err != nil {
			return err
		}
		col.tmpIdx[v] = col.index.Rows() - 1
	}
	col.tmpKey = append(col.tmpKey, col.tmpIdx[v])

	return nil
}

func (col *LowCardinality) Decode(decoder *binary.Decoder, _ int) error {
	keyVersion, err := decoder.UInt64()
	if err != nil {
		return err
	}
	if keyVersion != sharedDictionariesWithAdditionalKeys {
		return &LowCardinalityDecode{
			msg: "invalid key serialization version value",
		}
	}
	indexSerializationType, err := decoder.UInt64()
	if err != nil {
		return err
	}

	col.key = byte(indexSerializationType & indexTypeMask)
	switch col.key {
	case keyUInt8, keyUInt16, keyUInt32, keyUInt64:
	default:
		return &LowCardinalityDecode{
			msg: "invalid index serialization version value",
		}
	}
	switch {
	case indexSerializationType&needGlobalDictionaryBit == 1:
		return &LowCardinalityDecode{
			msg: "global dictionary is not supported",
		}
	case indexSerializationType&hasAdditionalKeysBit == 0:
		return &LowCardinalityDecode{
			msg: "additional keys bit is missing",
		}
	}
	indexRows, err := decoder.Int64()
	if err != nil {
		return err
	}
	if err := col.index.Decode(decoder, int(indexRows)); err != nil {
		return err
	}
	keysRows, err := decoder.Int64()
	if err != nil {
		return err
	}
	if err := col.keys().Decode(decoder, int(keysRows)); err != nil {
		return err
	}
	return nil
}

func (col *LowCardinality) Encode(encoder *binary.Encoder) error {
	defer func() {
		col.tmpIdx, col.tmpKey = nil, nil
	}()
	switch {
	case len(col.tmpKey) < math.MaxUint8:
		col.key = keyUInt8
		for _, v := range col.tmpKey {
			if err := col.keys8.AppendRow(uint8(v)); err != nil {
				return err
			}
		}
	case len(col.tmpKey) < math.MaxUint16:
		col.key = keyUInt16
		for _, v := range col.tmpKey {
			if err := col.keys16.AppendRow(uint16(v)); err != nil {
				return err
			}
		}
	case len(col.tmpKey) < math.MaxUint32:
		col.key = keyUInt32
		for _, v := range col.tmpKey {
			if err := col.keys32.AppendRow(uint32(v)); err != nil {
				return err
			}
		}
	default:
		col.key = keyUInt64
		for _, v := range col.tmpKey {
			if err := col.keys64.AppendRow(uint64(v)); err != nil {
				return err
			}
		}
	}
	if err := encoder.UInt64(sharedDictionariesWithAdditionalKeys); err != nil {
		return err
	}
	if err := encoder.UInt64(updateAll | uint64(col.key)); err != nil {
		return err
	}
	if err := encoder.Int64(int64(col.index.Rows())); err != nil {
		return err
	}
	if err := col.index.Encode(encoder); err != nil {
		return err
	}
	keys := col.keys()
	if err := encoder.Int64(int64(keys.Rows())); err != nil {
		return err
	}
	return keys.Encode(encoder)
}

func (col *LowCardinality) keys() Interface {
	switch col.key {
	case keyUInt8:
		return &col.keys8
	case keyUInt16:
		return &col.keys16
	case keyUInt32:
		return &col.keys32
	}
	return &col.keys64
}

func (col *LowCardinality) indexRowNum(row int) int {
	switch v := col.keys().Row(row, false).(type) {
	case uint8:
		return int(v)
	case uint16:
		return int(v)
	case uint32:
		return int(v)
	case uint64:
		return int(v)
	}
	return 0
}

var _ Interface = (*LowCardinality)(nil)
