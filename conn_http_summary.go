package clickhouse

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// summaryHeader carries the server's counts of a query on an HTTP response.
// The server sends it with the response headers, so it counts what the query
// did before its first block of data: all of a query that returns no data, as
// an INSERT or DDL, and all of a query that runs with wait_end_of_query = 1.
const summaryHeader = "X-ClickHouse-Summary"

// httpSummary is the JSON object of summaryHeader. The server writes each
// count as a quoted number.
type httpSummary struct {
	ReadRows        uint64 `json:"read_rows,string"`
	ReadBytes       uint64 `json:"read_bytes,string"`
	WrittenRows     uint64 `json:"written_rows,string"`
	WrittenBytes    uint64 `json:"written_bytes,string"`
	TotalRowsToRead uint64 `json:"total_rows_to_read,string"`
	ElapsedNs       uint64 `json:"elapsed_ns,string"`
}

// reportSummary gives summaryHeader to the WithProgress callback of the
// query, as the native protocol gives its progress packets.
func (h *httpConnect) reportSummary(header http.Header, options *QueryOptions) {
	if options == nil || options.events.progress == nil {
		return
	}
	value := header.Get(summaryHeader)
	if value == "" {
		return
	}
	var summary httpSummary
	if err := json.Unmarshal([]byte(value), &summary); err != nil {
		h.logger.Debug("HTTP summary header not read", slog.String("header", value), slog.Any("error", err))
		return
	}
	options.events.progress(&Progress{
		Rows:       summary.ReadRows,
		Bytes:      summary.ReadBytes,
		TotalRows:  summary.TotalRowsToRead,
		WroteRows:  summary.WrittenRows,
		WroteBytes: summary.WrittenBytes,
		Elapsed:    time.Duration(summary.ElapsedNs),
	})
}
