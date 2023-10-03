package clickhouse

import (
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func TestReadWithEmptyBlock(t *testing.T) {
	blockInitFunc := func() *proto.Block {
		retVal := &proto.Block{
			Packet:   0,
			Columns:  nil,
			Timezone: nil,
		}
		retVal.AddColumn("col1", ("Int64"))
		retVal.AddColumn("col2", ("String"))
		return retVal
	}

	testCases := map[string]struct {
		actual   func() rows
		expected int
	}{
		"none empty": {
			func() rows {
				firstBlock := blockInitFunc()
				firstBlock.Append(int64(0), strconv.Itoa(0))
				blockChan := make(chan *proto.Block)
				go func() {
					for i := 1; i < 10; i++ {
						block := blockInitFunc()
						block.Append(int64(i), strconv.Itoa(i))
						blockChan <- block
					}
					close(blockChan)
				}()
				return rows{
					err:       nil,
					row:       0,
					block:     firstBlock,
					totals:    nil,
					errors:    nil,
					stream:    blockChan,
					columns:   nil,
					structMap: nil,
				}
			},
			10,
		},
		"all empty": {
			func() rows {
				firstBlock := blockInitFunc()
				blockChan := make(chan *proto.Block)
				go func() {
					for i := 1; i < 10; i++ {
						block := blockInitFunc()
						blockChan <- block
					}
					close(blockChan)
				}()
				return rows{
					err:       nil,
					row:       0,
					block:     firstBlock,
					totals:    nil,
					errors:    nil,
					stream:    blockChan,
					columns:   nil,
					structMap: nil,
				}
			},
			0,
		},
		"some empty": {
			func() rows {
				firstBlock := blockInitFunc()
				blockChan := make(chan *proto.Block)
				go func() {
					for i := 1; i < 10; i++ {
						block := blockInitFunc()
						if i%2 == 0 {
							block.Append(int64(i), strconv.Itoa(i))
						}
						blockChan <- block
					}
					close(blockChan)
				}()
				return rows{
					err:       nil,
					row:       0,
					block:     firstBlock,
					totals:    nil,
					errors:    nil,
					stream:    blockChan,
					columns:   nil,
					structMap: nil,
				}
			},
			4,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := testCase.actual()

			rowCnt := 0
			for actual.Next() {
				rowCnt++
			}
			assert.Equal(t, testCase.expected, rowCnt)
		})
	}
}
