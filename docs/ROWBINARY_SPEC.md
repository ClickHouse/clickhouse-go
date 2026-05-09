# ClickHouse `RowBinary` Format Specification

> Derived from the encoders/decoders in `lib/column/` of `ClickHouse/clickhouse-go` v2 (which implements the **Native** TCP protocol). The per-value encodings used by `RowBinary` and `Native` are **identical**; the framing is different. Where they diverge (notably arrays and `LowCardinality`), this spec calls it out and describes the `RowBinary` form, not the Native one.

## 0. Important: this driver implements Native, not RowBinary

The `clickhouse-go` v2 driver writes/reads **Native** format blocks. `Native` and `RowBinary` share the same per-value type codecs (the body of every `Encode`/`Decode` method on a column in `lib/column/*.go`), but:

| Aspect | Native (this repo) | RowBinary |
|---|---|---|
| Layout | Columnar, in fixed-size blocks | Row-wise, no blocks |
| Framing per block | `num_columns` (uvarint), `num_rows` (uvarint), then per-column: name (string), type (string), `CustomSerialization` state prefix if any, then `num_rows` values | None — raw stream of rows |
| `Array(T)` | `num_rows` cumulative `UInt64` offsets, then values | Per-row: uvarint length, then that many values |
| `Map(K,V)` | `num_rows` cumulative `UInt64` offsets, then keys, then values | Per-row: uvarint length, then that many `(key,value)` pairs |
| `LowCardinality(T)` | Dictionary-and-keys protocol with multiple version flags | Encoded as if the underlying `T` (no dictionary on the wire) |
| `Nullable(T)` | `num_rows` `UInt8` null mask, then `num_rows` values of `T` | Per-row: 1 byte `is_null` (0/1), then a value of `T` (zero-valued if null) |

When implementing `RowBinary` for another language, take the per-value codecs in §3 below from this repo, but use the `RowBinary` framing rules from §1–§2.

There is also a `RowBinaryWithNames` variant (one varint count + varint-length-prefixed UTF‑8 column names header) and `RowBinaryWithNamesAndTypes` (additionally column type strings); the row body is identical.

## 1. Common encoding primitives

All multi-byte numeric values are **little-endian**. Two primitives are reused everywhere.

### 1.1 Unsigned varint (LEB128, uvarint)

Used for all length prefixes (string lengths, array lengths, map lengths, dynamic-type counts). 7 data bits per byte; the high bit (`0x80`) is set on every non-final byte. Maximum 10 bytes (uint64). This is `proto.Buffer.PutUVarInt` / `proto.Reader.UVarInt` in `ch-go`, and historically `lib/binary/uvarint.go`.

### 1.2 Length-prefixed string

`uvarint(len) || raw_bytes` — exactly `len` bytes follow. UTF‑8 is conventional but not enforced; bytes are passed through unchanged. Used for `String`, column names/type names in headers, JSON paths, etc. (`lib/column/string.go`, `ch-go` `proto.ColStr`.)

## 2. RowBinary framing

A `RowBinary` payload is a concatenation of N rows. Each row is the concatenation of one encoded value per column, in column order, using the per-type encoding from §3. There is no row separator, no row count, no checksum.

For `RowBinaryWithNames` / `RowBinaryWithNamesAndTypes`, prepend a header before the rows:

```
columns_count : uvarint
column_name[0..columns_count-1] : length-prefixed string
[ column_type[0..columns_count-1] : length-prefixed string ]   // only WithNamesAndTypes
```

## 3. Per-type encodings

Each subsection is the binary form of one value. All sizes are exact byte counts.

### 3.1 Fixed-width numerics (little-endian)

Source: `lib/column/column_gen.go`, `lib/column/bool.go`, `lib/column/bigint.go`.

| Type | Bytes | Notes |
|---|---|---|
| `Int8` / `UInt8` | 1 | |
| `Int16` / `UInt16` | 2 | |
| `Int32` / `UInt32` | 4 | |
| `Int64` / `UInt64` | 8 | |
| `Int128` / `UInt128` | 16 | Two LE-`uint64` halves: `low` then `high`. Signed values are two's complement of the full 128-bit integer. |
| `Int256` / `UInt256` | 32 | Four LE-`uint64` halves: `low.low, low.high, high.low, high.high`. Signed = two's complement. |
| `Float32` | 4 | IEEE 754 binary32 LE |
| `Float64` | 8 | IEEE 754 binary64 LE |
| `Bool` | 1 | `0x00` = false, `0x01` = true (stored as `UInt8`) |

### 3.2 `String`

`uvarint(len) || bytes`. Empty string is the single byte `0x00`. (`lib/column/string.go`.)

### 3.3 `FixedString(N)`

Exactly `N` bytes, no length prefix. Shorter inputs are right-padded with `0x00`; longer inputs are an error. (`lib/column/fixed_string.go`.)

### 3.4 `UUID`

16 bytes. **Important byte order**: ClickHouse stores a UUID as two `UInt64`s (each LE) — high 64 bits first, then low 64 bits. Practically this means: take the canonical 16-byte big-endian UUID, then byte-reverse each 8-byte half independently. (See `proto.ColUUID` used by `lib/column/uuid.go`.)

### 3.5 `Date`

`UInt16` — number of days since `1970-01-01` UTC. Range: `1970-01-01 .. 2149-06-06`. (`lib/column/date.go`.)

### 3.6 `Date32`

`Int32` — number of days since `1970-01-01` UTC (signed; supports dates before 1970). (`lib/column/date32.go`.)

### 3.7 `DateTime` / `DateTime([timezone])`

`UInt32` — Unix seconds (UTC). Timezone, if present in the type, is **display-only metadata** and does not affect the wire bytes. (`lib/column/datetime.go`.)

### 3.8 `DateTime64(P[, timezone])`

`Int64` — ticks of `10^-P` seconds since the Unix epoch (signed; supports dates before 1970). `P` is the precision parameter from the type (`0..9`). Timezone is display-only. (`lib/column/datetime64.go`.)

### 3.9 `Time` / `Time64(P)`

`Time` is `Int32` seconds; `Time64(P)` is `Int64` ticks of `10^-P` seconds. They represent a duration/time-of-day, not an absolute timestamp. (`lib/column/time.go`, `lib/column/time64.go`.)

### 3.10 `Decimal(P, S)`

Stored as a **signed** integer of width determined by `P`, two's complement, little-endian, with the value being `unscaled = round(real_value × 10^S)`:

| Precision `P` | Storage |
|---|---|
| `1..9` | `Int32` (4 bytes) — `Decimal32` |
| `10..18` | `Int64` (8 bytes) — `Decimal64` |
| `19..38` | `Int128` (16 bytes) — `Decimal128` |
| `39..76` | `Int256` (32 bytes) — `Decimal256` |

(`lib/column/decimal.go`.)

### 3.11 `Enum8` / `Enum16`

Exactly `Int8` / `Int16` on the wire. The string ↔ integer mapping is part of the type definition, not the value. (`lib/column/enum.go`, `lib/column/enum8.go`, `lib/column/enum16.go`.)

### 3.12 `IPv4`

`UInt32` containing the IPv4 address. ClickHouse stores it in **network byte order interpreted as little-endian uint32** — i.e. the four address octets `a.b.c.d` are laid out on the wire as `d, c, b, a`. (`lib/column/ipv4.go`, `proto.ColIPv4`.)

### 3.13 `IPv6`

16 raw bytes in network byte order (the canonical IPv6 representation). (`lib/column/ipv6.go`.)

### 3.14 `Nullable(T)` — RowBinary form

```
is_null : UInt8     // 0 = present, 1 = null
value   : T         // ALWAYS present; if is_null=1 a default-typed value (e.g. zero) is written and ignored on read
```

Note: this differs from Native, which writes a separate null mask block before the values. Source for the per-value semantics: `lib/column/nullable.go` (`Append`/`AppendRow` produce a `nulls []uint8` parallel to the values).

### 3.15 `Array(T)` — RowBinary form

```
n     : uvarint
v[i]  : T          // for i in 0..n-1
```

For `Array(Array(...))` apply recursively per element.

(In `lib/column/array.go` you instead see cumulative `UInt64` offsets — that is Native framing only.)

### 3.16 `Tuple(T1, T2, ..., Tn)`

Concatenation of one value of each element type, in declared order, no separators, no length prefix. (`lib/column/tuple.go`.)

### 3.17 `Map(K, V)` — RowBinary form

```
n              : uvarint
(k[i], v[i])   : K then V    // for i in 0..n-1
```

Keys are unique within a row; ordering is preserved as written. (`lib/column/map.go`.)

### 3.18 `LowCardinality(T)` — RowBinary form

In RowBinary, `LowCardinality(T)` is encoded **as if the column were `T`**: write/read each value directly using the underlying type's codec. There is no dictionary, no key-width selection, and no state prefix.

The complex protocol you see in `lib/column/lowcardinality.go` (state prefix `sharedDictionariesWithAdditionalKeys = 1`, the per-block `indexSerializationType` UInt64 with bits `hasAdditionalKeysBit (1<<9)`, `needGlobalDictionaryBit (1<<8)`, `needUpdateDictionary (1<<10)`, key widths `keyUInt8/16/32/64`, dictionary size `UInt64`, dictionary values, then keys count `UInt64` and key indices) is **Native-only**. Do not reproduce it for RowBinary.

For `LowCardinality(Nullable(T))` in RowBinary, encode as `Nullable(T)` (i.e., null-flag byte + value of T).

### 3.19 Geo types

These are deterministic compositions of the primitives above (`lib/column/geo_*.go`):

- `Point` = `Tuple(Float64, Float64)` → 16 bytes: `x` (lon) then `y` (lat).
- `Ring` = `Array(Point)`.
- `LineString` = `Array(Point)`.
- `Polygon` = `Array(Ring)`.
- `MultiLineString` = `Array(LineString)`.
- `MultiPolygon` = `Array(Polygon)`.

### 3.20 `Interval*`

Wire format is `Int64` (count of the unit named in the type, e.g. `IntervalSecond`). Note: ClickHouse does not allow storing `Interval*` in tables; in practice you encounter them only in computed query results. (`lib/column/interval.go`.)

### 3.21 `Nested(...)`

A `Nested(name1 T1, name2 T2, ...)` column behaves on the wire exactly like `Tuple(Array(T1), Array(T2), ...)` with the additional invariant that all those arrays have equal length per row. Encode/decode using §3.15 and §3.16. (`lib/column/nested.go`.)

### 3.22 `Variant(T1, T2, ..., Tn)` — RowBinary form

Per row:

```
discriminator : UInt8         // 0..n-1 selects the active type; 255 means NULL
value         : T_discriminator     // omitted when discriminator == 255
```

The variant alternatives are sorted by ClickHouse into a canonical order; the discriminator indexes that canonical order, not the order written in the schema. The Native format additionally has a `WriteStatePrefix` writing `UInt64(SupportedVariantSerializationVersion = 0)` before any variant column in a block — RowBinary has no such prefix. Constants and structure: `lib/column/variant.go` (`SupportedVariantSerializationVersion = 0`, `VariantNullDiscriminator = 255`).

### 3.23 `Dynamic` — RowBinary form

`Dynamic` carries its own type per row. For each row:

```
type_name : length-prefixed string         // empty string ("") means NULL
value     : encoded according to type_name // omitted when type_name is ""
```

`type_name` uses ClickHouse type syntax (e.g. `"Int64"`, `"String"`, `"Array(UInt32)"`). Once a row's type is known, the value bytes are exactly the per-type encoding from §3.

The Native form is dramatically more complex (a per-block header with serialization version `3` (current) or `1` (deprecated v1), a `total_types` uvarint, a list of variant type names, then per-row a width-adapted discriminator (`UInt8/16/32/64` chosen by `total_types`), then the underlying typed columns). See `lib/column/dynamic.go` constants `DynamicSerializationVersion = 3`, `DynamicDeprecatedSerializationVersion = 1`, `DynamicNullDiscriminator = -1`, `DefaultMaxDynamicTypes = 32`. None of that header machinery applies to RowBinary.

### 3.24 `JSON` — RowBinary form

In RowBinary, a `JSON` value is encoded as a **`String`** (§3.2) containing the JSON text. The literal `"null"` (4 bytes after the length prefix) represents a SQL `NULL` for an all-null context.

The driver supports two Native-protocol modes per block — string mode (version `1`) and structured object mode (version `3`) — but those modes apply only to Native blocks. They are signalled by a `UInt64` serialization version emitted from `WriteStatePrefix` (see `lib/column/json.go`: `JSONStringSerializationVersion = 1`, `JSONObjectSerializationVersion = 3`, deprecated object `0`). For an external client building a `RowBinary` parser/writer, treat `JSON` as `String`.

### 3.25 `SimpleAggregateFunction(func, T)`

On the wire, encoded exactly as `T`. The aggregate-function name is type metadata only. (`lib/column/simple_aggregate_function.go`.)

### 3.26 `AggregateFunction(...)`

This driver does not encode `AggregateFunction` values; ClickHouse generally exposes them as opaque binary buffers (`String`-shaped). Treat as out of scope unless your application needs it.

## 4. Reference: lookup for column implementations

When in doubt about a type's bytes, the canonical reference is the `Encode`/`Decode` pair (and underlying `proto.Col*` from `github.com/ClickHouse/ch-go`) for that type in `lib/column/`:

| ClickHouse type | File |
|---|---|
| Integers, floats, bool | `column_gen.go`, `bool.go` |
| Int128/256, UInt128/256 | `bigint.go` |
| String / FixedString | `string.go`, `fixed_string.go` |
| UUID | `uuid.go` |
| Date / Date32 | `date.go`, `date32.go` |
| DateTime / DateTime64 | `datetime.go`, `datetime64.go` |
| Time / Time64 | `time.go`, `time64.go` |
| Decimal | `decimal.go` |
| Enum8 / Enum16 | `enum.go`, `enum8.go`, `enum16.go` |
| IPv4 / IPv6 | `ipv4.go`, `ipv6.go` |
| Nullable | `nullable.go` |
| Array | `array.go` |
| Tuple / Nested | `tuple.go`, `nested.go` |
| Map | `map.go` |
| LowCardinality | `lowcardinality.go` (Native-only framing) |
| Geo | `geo_point.go`, `geo_ring.go`, `geo_polygon.go`, `geo_multi_polygon.go`, `geo_linestring.go`, `geo_multi_linestring.go` |
| Interval | `interval.go` |
| Variant | `variant.go` |
| Dynamic | `dynamic.go`, `dynamic_deprecated.go` |
| JSON | `json.go`, `json_deprecated.go` |
| SimpleAggregateFunction | `simple_aggregate_function.go` |

## 5. Implementation checklist for a new RowBinary client

1. Implement uvarint read/write and a length-prefixed string codec.
2. Implement little-endian fixed-width numeric codecs from §3.1.
3. Implement composite codecs from §3.14–§3.18 in terms of the primitives. Remember: `Nullable` = `is_null` byte + value; `Array`/`Map` = uvarint length + elements; `LowCardinality` is transparent (encode as `T`).
4. For `Variant` / `Dynamic`, follow §3.22–§3.23 (single discriminator/type-name per row, no block-level state prefix).
5. For `JSON` in RowBinary, use §3.2 (a length-prefixed JSON text string).
6. For `RowBinaryWithNames` / `RowBinaryWithNamesAndTypes`, emit/parse the header in §2 before the rows.
7. Cross-test against ClickHouse with `FORMAT RowBinary` and `INSERT ... FORMAT RowBinary` — every type can be round-tripped using a single-column table.

That's the whole format. Once you have uvarint + LE numerics + length-prefixed string, every other type in §3 is a one- or two-line composition.
