package clickhouse

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockToJSONEachRow(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("id", column.Type("UInt64")))
	require.NoError(t, block.AddColumn("name", column.Type("String")))
	require.NoError(t, block.AddColumn("score", column.Type("Float64")))

	require.NoError(t, block.Append(uint64(1), "alice", float64(95.5)))
	require.NoError(t, block.Append(uint64(2), "bob", float64(87.3)))
	require.NoError(t, block.Append(uint64(3), "charlie", float64(92.1)))

	data, err := blockToJSONEachRow(block)
	require.NoError(t, err)

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	assert.Len(t, lines, 3)

	// Parse each line and verify
	var row1 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &row1))
	assert.Equal(t, float64(1), row1["id"]) // JSON numbers are float64
	assert.Equal(t, "alice", row1["name"])
	assert.Equal(t, 95.5, row1["score"])

	var row2 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &row2))
	assert.Equal(t, float64(2), row2["id"])
	assert.Equal(t, "bob", row2["name"])

	var row3 map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[2]), &row3))
	assert.Equal(t, float64(3), row3["id"])
	assert.Equal(t, "charlie", row3["name"])
}

func TestBlockToJSONEachRowEmpty(t *testing.T) {
	block := proto.NewBlock()
	require.NoError(t, block.AddColumn("id", column.Type("UInt64")))

	data, err := blockToJSONEachRow(block)
	assert.NoError(t, err)
	assert.Nil(t, data)
}

func TestInsertFormatOption(t *testing.T) {
	opts := driver.PrepareBatchOptions{}
	assert.Equal(t, driver.InsertFormatNative, opts.InsertFormat)

	driver.WithInsertFormat(driver.InsertFormatJSONEachRow)(&opts)
	assert.Equal(t, driver.InsertFormatJSONEachRow, opts.InsertFormat)
}

func TestFormatJSONValue(t *testing.T) {
	// nil
	assert.Nil(t, formatJSONValue(nil))

	// []byte → string
	assert.Equal(t, "hello", formatJSONValue([]byte("hello")))

	// primitive types pass through
	assert.Equal(t, int64(42), formatJSONValue(int64(42)))
	assert.Equal(t, "test", formatJSONValue("test"))
	assert.Equal(t, true, formatJSONValue(true))
	assert.Equal(t, 3.14, formatJSONValue(3.14))
}
