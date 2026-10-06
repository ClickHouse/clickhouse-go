# Native server-side formats example

This example uses only the native TCP protocol, and no client-side format library, in both directions:

- `QueryFormat` asks ClickHouse to encode a query result as `CSV`, `Parquet`, and `Arrow`, and the returned bytes are written directly to files.
- `InsertFormat` streams every file unchanged back to ClickHouse, which parses it on the server.

It then compares a row count and checksum calculated by ClickHouse. A successful round trip proves that each output is accepted by the corresponding ClickHouse input-format parser.

Run it against a server containing the native server-formatted-results extension:

```bash
go run ./examples/server_side_formats
```

Defaults:

- Native TCP: `localhost:9000`
- Database: `default`
- User: `default`
- Output directory: `server-formatted-output`

The following environment variables override those values:

```bash
CLICKHOUSE_NATIVE_ADDR=localhost:9000 \
CLICKHOUSE_DATABASE=default \
CLICKHOUSE_USER=default \
CLICKHOUSE_PASSWORD=secret \
CLICKHOUSE_FORMAT_OUTPUT_DIR=server-formatted-output \
go run ./examples/server_side_formats
```

The output files remain on disk as `result.csv`, `result.parquet`, and `result.arrow`. The imported `Memory` tables also remain available as:

- `clickhouse_go_server_format_demo_csv`
- `clickhouse_go_server_format_demo_parquet`
- `clickhouse_go_server_format_demo_arrow`

The example recreates those three tables on each run.
