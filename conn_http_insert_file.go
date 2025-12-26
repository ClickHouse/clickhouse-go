package clickhouse

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
)

var contentEncodingExtensions = map[string][]string{
	"gzip":    {".gz", ".gzip"},
	"br":      {".br", ".brotli"},
	"deflate": {".deflate"},
	"xz":      {".xz"},
	"zstd":    {".zst", ".zstd"},
	"lz4":     {".lz", ".lz4"},
	"bz2":     {".bz2"},
	"snappy":  {".snappy"},
}

// insertFile streams a local file directly to ClickHouse over HTTP as the request body.
//
// The file is sent "as-is" without any decompression or recompression on the client side.
// This method is intended for INSERT ... FORMAT <fmt> queries where the payload is already
// prepared and optionally compressed (e.g. TSV.zst).
//
// Compression handling:
//   - If contentEncoding is explicitly provided, it is set as HTTP "Content-Encoding".
//   - If contentEncoding is empty, the encoding is auto-detected from the file extension
//     (e.g. ".zst" → "zstd", ".gz" → "gzip").
//   - The driver does NOT attempt to decode, encode, or transform the stream.
//
// Parameters:
//   - ctx: request context (cancellation, deadlines).
//   - filePath: path to the file to upload; the file is streamed and not buffered in memory.
//   - query: ClickHouse INSERT query (typically "INSERT INTO <table> FORMAT <format>").
//
// Limitations:
//   - External tables are not supported for file uploads.
//   - This method is available only for the HTTP transport.
//
// Typical usage:
// err := conn.insertFile(ctx, "data.tsv.zst", "text/tab-separated-values", "zstd", "INSERT INTO db.table FORMAT TSV")
//
// On success, the file contents are fully consumed and the request body is discarded.
func (h *httpConnect) insertFile(ctx context.Context, filePath string, query string) error {
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return err
	}
	st.Mode().IsRegular()

	options := queryOptions(ctx)
	options.settings["query"] = query

	if len(options.external) > 0 {
		return fmt.Errorf("external tables are not supported for file upload")
	}
	if options.fileEncoding == "" {
		options.fileEncoding = encodingFromFileName(f.Name())
	}
	if options.fileContentType == "" {
		options.fileContentType = contentTypeFromFormat(parseFormatFromSQL(query))
		if options.fileContentType == "" {
			return fmt.Errorf("unknown file Content-Type")
		}
	}

	headers := map[string]string{"Content-Type": options.fileContentType}
	if options.fileEncoding != "" {
		headers["Content-Encoding"] = options.fileEncoding
	}

	switch h.compression {
	case CompressionZSTD, CompressionLZ4:
		options.settings["compress"] = "1"
	case CompressionGZIP, CompressionDeflate, CompressionBrotli:
		// request encoding
		headers["Accept-Encoding"] = h.compression.String()
	}

	req, err := h.createRequest(ctx, h.url.String(), f, &options, headers)
	if err != nil {
		return err
	}

	res, err := h.executeRequest(req)
	if err != nil {
		return err
	}
	defer discardAndClose(res.Body)

	return nil
}

func encodingFromFileName(fileName string) string {
	extentions := map[string][]string{
		"gzip":    {".gz", ".gzip"},
		"br":      {".br", ".brotli"},
		"deflate": {".deflate"},
		"xz":      {".xz"},
		"zstd":    {".zst", ".zstd"},
		"lz4":     {".lz", ".lz4"},
		"bz2":     {".bz2"},
		"snappy":  {".snappy"},
	}

	for encoding, extention := range extentions {
		for _, ext := range extention {
			if strings.HasSuffix(fileName, ext) {
				return encoding
			}
		}
	}

	return ""
}

func parseFormatFromSQL(query string) string {
	re := regexp.MustCompile(`(?i)\bformat\b\s*([A-Za-z0-9_]+)`)
	m := re.FindStringSubmatch(query)
	if len(m) > 1 {
		return m[1]
	}
	return ""
}

func contentTypeFromFormat(format string) string {
	formats := map[string][]string{
		"text/tab-separated-values": {
			"TabSeparated", "TSV",
			"TabSeparatedRaw", "TSVRaw", "Raw",
			"TabSeparatedWithNames", "TSVWithNames", "RawWithNames",
			"TabSeparatedWithNamesAndTypes", "TSVWithNamesAndTypes", "RawWithNamesAndTypes",
			"TabSeparatedRawWithNames", "TSVRawWithNames", "RawWithNames",
			"TabSeparatedRawWithNamesAndTypes", "TSVRawWithNamesAndNames", "RawWithNamesAndNames",
		},
		"text/csv": {"CSV", "CSVWithNames", "CSVWithNamesAndTypes"},
		"application/json": {
			"JSON", "JSONAsString", "JSONAsObject", "JSONStrings", "JSONColumns", "JSONColumnsWithMetadata", "JSONObjectEachRow",
			"JSONEachRow", "PrettyJSONEachRow", "JSONEachRowWithProgress", "JSONStringsEachRow", "JSONStringsEachRowWithProgress",
			"JSONCompact", "JSONCompactStrings", "JSONCompactColumns", "JSONCompactEachRow", "JSONCompactEachRowWithNames",
			"JSONCompactEachRowWithNamesAndTypes", "JSONCompactEachRowWithProgress", "JSONCompactStringsEachRow",
			"JSONCompactStringsEachRowWithNames", "JSONCompactStringsEachRowWithNamesAndTypes", "JSONCompactStringsEachRowWithProgress",
		},
	}

	for contentType, fmts := range formats {
		for _, fmt := range fmts {
			if strings.ToLower(fmt) == strings.ToLower(format) {
				return contentType
			}
		}
	}

	return "application/octet-stream"
}
