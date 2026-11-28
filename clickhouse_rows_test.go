package clickhouse

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestReadWithEmptyBlock(t *testing.T) {
	blockInitFunc := func() *proto.Block {
		retVal := &proto.Block{
			Packet:        0,
			Columns:       nil,
			ServerContext: &column.ServerContext{},
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

func TestRowScanErrorDrainsRows(t *testing.T) {
	newBlock := func(t *testing.T, values ...any) *proto.Block {
		block := &proto.Block{ServerContext: &column.ServerContext{}}
		require.NoError(t, block.AddColumn("value", "Nullable(String)"))
		for _, v := range values {
			require.NoError(t, block.Append(v))
		}
		return block
	}

	scans := map[string]struct {
		scan    func(r *row) error
		wantErr string
	}{
		"Scan": {
			scan: func(r *row) error {
				var value string
				return r.Scan(&value)
			},
			wantErr: "converting NULL to *string is unsupported",
		},
		"ScanStruct": {
			scan: func(r *row) error {
				var dest struct {
					Other string `ch:"other"`
				}
				return r.ScanStruct(&dest)
			},
			wantErr: `missing destination name "value"`,
		},
	}

	for name, tc := range scans {
		t.Run(name, func(t *testing.T) {
			// The first value is NULL, so Scan into a string fails on the first row. The
			// stream holds more blocks than its buffer, like a large result.
			blocks := []*proto.Block{newBlock(t, nil), newBlock(t, "a"), newBlock(t, "b"), newBlock(t, "c")}
			stream := make(chan *proto.Block, 2)
			errs := make(chan error)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for _, block := range blocks {
					stream <- block
				}
				close(stream)
				close(errs)
			}()

			header := newBlock(t)
			r := &row{rows: &rows{
				block:     header,
				columns:   header.ColumnsNames(),
				structMap: &structMap{},
				stream:    stream,
				errors:    errs,
			}}
			require.ErrorContains(t, tc.scan(r), tc.wantErr)

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("rows were not drained after the failed scan, so the connection would not be released")
			}
		})
	}
}
