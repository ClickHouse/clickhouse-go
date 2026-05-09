// rowbinary.js — single-file draft of a ClickHouse RowBinary codec in JS.
//
// Scope: PLAIN (scalar) types only. Composite types (Nullable, Array, Tuple,
// Map, LowCardinality, Variant, Dynamic, JSON, Nested, Geo) are intentionally
// out of scope for this draft and will be added in a follow-up.
//
// Spec reference: docs/ROWBINARY_SPEC.md (sections 1, 3.1–3.13).
//
// Design:
//   * `Reader` wraps a Uint8Array + cursor, exposes one method per primitive.
//   * `Writer` accumulates Uint8Array chunks, exposes one method per primitive.
//   * `Codecs[typeName]` returns `{ read(reader), write(writer, value) }` for a
//     given ClickHouse type spelling. Parametric types (`FixedString(N)`,
//     `DateTime64(P)`, `Decimal(P,S)`, `Enum8(...)`, etc.) are produced by
//     calling `codecFor("DateTime64(3)")`.
//
// Numeric representation:
//   * Int8..Int32, UInt8..UInt32, Float32/64 -> JS Number.
//   * Int64/UInt64, Int128/UInt128, Int256/UInt256, Decimal* unscaled -> BigInt.
//   * Date/Date32/DateTime/DateTime64/Time/Time64 -> JS Number (Date* in days,
//     DateTime* in seconds, Time64/DateTime64 unscaled ticks via BigInt for P>0
//     to avoid precision loss; see notes per codec).
//
// Run `node docs/rowbinary.js` to execute the round-trip self-test at the
// bottom of this file.

'use strict';

// ---------------------------------------------------------------------------
// Reader / Writer
// ---------------------------------------------------------------------------

class Reader {
    constructor(bytes) {
        if (!(bytes instanceof Uint8Array)) {
            throw new TypeError('Reader expects a Uint8Array');
        }
        this.bytes = bytes;
        this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
        this.pos = 0;
    }

    _need(n) {
        if (this.pos + n > this.bytes.length) {
            throw new RangeError(`RowBinary: short read (need ${n} at ${this.pos}, have ${this.bytes.length})`);
        }
    }

    readBytes(n) {
        this._need(n);
        const out = this.bytes.subarray(this.pos, this.pos + n);
        this.pos += n;
        return out;
    }

    readUVarInt() {
        let result = 0n;
        let shift = 0n;
        for (let i = 0; i < 10; i++) {
            this._need(1);
            const b = this.bytes[this.pos++];
            result |= BigInt(b & 0x7f) << shift;
            if ((b & 0x80) === 0) {
                // Length prefixes practically fit in a Number; the caller may
                // cast to BigInt if it knows otherwise.
                if (result <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(result);
                return result;
            }
            shift += 7n;
        }
        throw new RangeError('RowBinary: uvarint overflow (>10 bytes)');
    }

    readU8()   { this._need(1); return this.view.getUint8(this.pos++); }
    readI8()   { this._need(1); return this.view.getInt8(this.pos++); }
    readU16()  { this._need(2); const v = this.view.getUint16(this.pos, true); this.pos += 2; return v; }
    readI16()  { this._need(2); const v = this.view.getInt16(this.pos, true);  this.pos += 2; return v; }
    readU32()  { this._need(4); const v = this.view.getUint32(this.pos, true); this.pos += 4; return v; }
    readI32()  { this._need(4); const v = this.view.getInt32(this.pos, true);  this.pos += 4; return v; }
    readU64()  { this._need(8); const v = this.view.getBigUint64(this.pos, true); this.pos += 8; return v; }
    readI64()  { this._need(8); const v = this.view.getBigInt64(this.pos, true);  this.pos += 8; return v; }
    readF32()  { this._need(4); const v = this.view.getFloat32(this.pos, true); this.pos += 4; return v; }
    readF64()  { this._need(8); const v = this.view.getFloat64(this.pos, true); this.pos += 8; return v; }
}

class Writer {
    constructor() {
        this.chunks = [];
        this.length = 0;
    }

    _push(buf) {
        this.chunks.push(buf);
        this.length += buf.byteLength;
    }

    bytes() {
        const out = new Uint8Array(this.length);
        let off = 0;
        for (const c of this.chunks) {
            out.set(c, off);
            off += c.byteLength;
        }
        return out;
    }

    writeBytes(b) { this._push(b instanceof Uint8Array ? b : new Uint8Array(b)); }

    writeUVarInt(value) {
        let v = typeof value === 'bigint' ? value : BigInt(value);
        if (v < 0n) throw new RangeError('uvarint must be non-negative');
        const buf = [];
        while (v >= 0x80n) {
            buf.push(Number((v & 0x7fn) | 0x80n));
            v >>= 7n;
        }
        buf.push(Number(v));
        this._push(Uint8Array.from(buf));
    }

    _scratch(n, fill) {
        const b = new Uint8Array(n);
        const dv = new DataView(b.buffer);
        fill(dv);
        this._push(b);
    }

    writeU8(v)  { this._scratch(1, dv => dv.setUint8(0, v)); }
    writeI8(v)  { this._scratch(1, dv => dv.setInt8(0, v)); }
    writeU16(v) { this._scratch(2, dv => dv.setUint16(0, v, true)); }
    writeI16(v) { this._scratch(2, dv => dv.setInt16(0, v, true)); }
    writeU32(v) { this._scratch(4, dv => dv.setUint32(0, v, true)); }
    writeI32(v) { this._scratch(4, dv => dv.setInt32(0, v, true)); }
    writeU64(v) { this._scratch(8, dv => dv.setBigUint64(0, BigInt(v), true)); }
    writeI64(v) { this._scratch(8, dv => dv.setBigInt64(0, BigInt(v), true)); }
    writeF32(v) { this._scratch(4, dv => dv.setFloat32(0, v, true)); }
    writeF64(v) { this._scratch(8, dv => dv.setFloat64(0, v, true)); }
}

// ---------------------------------------------------------------------------
// Helpers for big integers wider than 64 bits
// ---------------------------------------------------------------------------

const TWO64 = 1n << 64n;
const TWO128 = 1n << 128n;
const TWO256 = 1n << 256n;

function writeBigUintLE(writer, value, byteCount) {
    if (typeof value !== 'bigint') value = BigInt(value);
    const mod = 1n << BigInt(byteCount * 8);
    if (value < 0n) value = (mod + (value % mod)) % mod; // two's complement
    if (value >= mod) throw new RangeError(`value does not fit in ${byteCount} bytes`);
    const buf = new Uint8Array(byteCount);
    for (let i = 0; i < byteCount; i++) {
        buf[i] = Number(value & 0xffn);
        value >>= 8n;
    }
    writer.writeBytes(buf);
}

function readBigUintLE(reader, byteCount) {
    const bytes = reader.readBytes(byteCount);
    let v = 0n;
    for (let i = byteCount - 1; i >= 0; i--) {
        v = (v << 8n) | BigInt(bytes[i]);
    }
    return v;
}

function readBigIntLE(reader, byteCount) {
    const u = readBigUintLE(reader, byteCount);
    const signBit = 1n << BigInt(byteCount * 8 - 1);
    return u >= signBit ? u - (1n << BigInt(byteCount * 8)) : u;
}

// ---------------------------------------------------------------------------
// Codec table for plain types
// ---------------------------------------------------------------------------

const TEXT_DEC = new TextDecoder('utf-8');
const TEXT_ENC = new TextEncoder();

const fixedCodecs = {
    Int8:    { read: r => r.readI8(),  write: (w, v) => w.writeI8(v) },
    UInt8:   { read: r => r.readU8(),  write: (w, v) => w.writeU8(v) },
    Int16:   { read: r => r.readI16(), write: (w, v) => w.writeI16(v) },
    UInt16:  { read: r => r.readU16(), write: (w, v) => w.writeU16(v) },
    Int32:   { read: r => r.readI32(), write: (w, v) => w.writeI32(v) },
    UInt32:  { read: r => r.readU32(), write: (w, v) => w.writeU32(v) },
    Int64:   { read: r => r.readI64(), write: (w, v) => w.writeI64(v) },
    UInt64:  { read: r => r.readU64(), write: (w, v) => w.writeU64(v) },
    Float32: { read: r => r.readF32(), write: (w, v) => w.writeF32(v) },
    Float64: { read: r => r.readF64(), write: (w, v) => w.writeF64(v) },

    // Bool is wire-identical to UInt8 with values 0/1.
    Bool: {
        read: r => r.readU8() !== 0,
        write: (w, v) => w.writeU8(v ? 1 : 0),
    },

    // Wide integers — BigInt in/out.
    Int128:  { read: r => readBigIntLE(r, 16), write: (w, v) => writeBigUintLE(w, v, 16) },
    UInt128: { read: r => readBigUintLE(r, 16), write: (w, v) => writeBigUintLE(w, v, 16) },
    Int256:  { read: r => readBigIntLE(r, 32), write: (w, v) => writeBigUintLE(w, v, 32) },
    UInt256: { read: r => readBigUintLE(r, 32), write: (w, v) => writeBigUintLE(w, v, 32) },

    // String: uvarint(len) || bytes. JS string in/out (UTF-8).
    String: {
        read: r => {
            const n = r.readUVarInt();
            return TEXT_DEC.decode(r.readBytes(n));
        },
        write: (w, v) => {
            const buf = typeof v === 'string' ? TEXT_ENC.encode(v) : v;
            w.writeUVarInt(buf.byteLength);
            w.writeBytes(buf);
        },
    },

    // UUID: stored as two LE UInt64 halves with the HIGH 64 bits FIRST.
    // Canonical text form is preserved on the JS side.
    UUID: {
        read: r => {
            const hi = r.readU64();
            const lo = r.readU64();
            return uuidToString((hi << 64n) | lo);
        },
        write: (w, v) => {
            const big = uuidFromString(v);
            const hi = big >> 64n;
            const lo = big & ((1n << 64n) - 1n);
            w.writeU64(hi);
            w.writeU64(lo);
        },
    },

    // Date  -> JS Date at UTC midnight (days since 1970-01-01).
    Date: {
        read: r => new Date(r.readU16() * 86400000),
        write: (w, v) => w.writeU16(daysSinceEpoch(v)),
    },
    Date32: {
        read: r => new Date(r.readI32() * 86400000),
        write: (w, v) => w.writeI32(daysSinceEpoch(v)),
    },

    // DateTime -> JS Date (Unix seconds). Timezone in the type is metadata.
    DateTime: {
        read: r => new Date(r.readU32() * 1000),
        write: (w, v) => w.writeU32(unixSeconds(v)),
    },

    // Time / Time64(0): seconds-of-day as a plain Number.
    Time: {
        read: r => r.readI32(),
        write: (w, v) => w.writeI32(v | 0),
    },

    // IPv4: stored as UInt32, but the on-wire bytes are the address octets in
    // REVERSE order (d,c,b,a for "a.b.c.d"). Round-trip via dotted-quad string.
    IPv4: {
        read: r => {
            const b = r.readBytes(4);
            return `${b[3]}.${b[2]}.${b[1]}.${b[0]}`;
        },
        write: (w, v) => {
            const parts = String(v).split('.').map(Number);
            if (parts.length !== 4 || parts.some(p => !(p >= 0 && p <= 255))) {
                throw new RangeError(`invalid IPv4: ${v}`);
            }
            w.writeBytes(Uint8Array.from([parts[3], parts[2], parts[1], parts[0]]));
        },
    },

    // IPv6: 16 raw bytes in network byte order.
    IPv6: {
        read: r => ipv6BytesToString(r.readBytes(16)),
        write: (w, v) => w.writeBytes(ipv6StringToBytes(v)),
    },
};

// FixedString(N): exactly N bytes, no length prefix, right-padded with 0x00.
function fixedStringCodec(n) {
    if (!Number.isInteger(n) || n <= 0) throw new RangeError('FixedString N must be a positive integer');
    return {
        read: r => r.readBytes(n).slice(),
        write: (w, v) => {
            let buf;
            if (v instanceof Uint8Array) buf = v;
            else if (typeof v === 'string') buf = TEXT_ENC.encode(v);
            else throw new TypeError('FixedString value must be string or Uint8Array');
            if (buf.byteLength > n) throw new RangeError(`FixedString(${n}) overflow: ${buf.byteLength}`);
            const padded = new Uint8Array(n);
            padded.set(buf, 0);
            w.writeBytes(padded);
        },
    };
}

// DateTime64(P[, tz]): Int64 ticks of 10^-P seconds.
// Returns/accepts BigInt ticks (avoids precision loss for P>=4).
function dateTime64Codec(p) {
    if (!Number.isInteger(p) || p < 0 || p > 9) throw new RangeError('DateTime64 P must be 0..9');
    return {
        read: r => r.readI64(),
        write: (w, v) => w.writeI64(typeof v === 'bigint' ? v : BigInt(v)),
    };
}

// Time64(P): Int64 ticks of 10^-P seconds-of-day.
function time64Codec(p) {
    if (!Number.isInteger(p) || p < 0 || p > 9) throw new RangeError('Time64 P must be 0..9');
    return {
        read: r => r.readI64(),
        write: (w, v) => w.writeI64(typeof v === 'bigint' ? v : BigInt(v)),
    };
}

// Decimal(P, S): signed integer storage of width determined by P. Value is the
// unscaled integer (BigInt). Caller is responsible for the decimal point.
function decimalCodec(p /*, s -- scale is metadata only on the wire */) {
    let bytes;
    if (p >= 1 && p <= 9)        bytes = 4;
    else if (p >= 10 && p <= 18) bytes = 8;
    else if (p >= 19 && p <= 38) bytes = 16;
    else if (p >= 39 && p <= 76) bytes = 32;
    else throw new RangeError(`Decimal precision must be 1..76, got ${p}`);
    return {
        read: r => readBigIntLE(r, bytes),
        write: (w, v) => writeBigUintLE(w, v, bytes),
    };
}

// Enum8 / Enum16: wire-identical to Int8 / Int16. We don't carry the name map.
function enumCodec(width) {
    return width === 1 ? fixedCodecs.Int8 : fixedCodecs.Int16;
}

// ---------------------------------------------------------------------------
// Type-string parser (only as much as plain types need)
// ---------------------------------------------------------------------------

function codecFor(type) {
    if (typeof type !== 'string') throw new TypeError('type must be a string');
    const t = type.trim();

    if (Object.prototype.hasOwnProperty.call(fixedCodecs, t)) return fixedCodecs[t];

    // Parameterised forms.
    let m;
    if ((m = /^FixedString\(\s*(\d+)\s*\)$/.exec(t)))   return fixedStringCodec(Number(m[1]));
    if ((m = /^DateTime64\(\s*(\d+)\s*(?:,[^)]*)?\)$/.exec(t))) return dateTime64Codec(Number(m[1]));
    if ((m = /^DateTime\(\s*[^)]*\)$/.exec(t)))         return fixedCodecs.DateTime;
    if ((m = /^Time64\(\s*(\d+)\s*\)$/.exec(t)))        return time64Codec(Number(m[1]));
    if ((m = /^Decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$/.exec(t))) return decimalCodec(Number(m[1]), Number(m[2]));
    if ((m = /^Decimal32\(\s*(\d+)\s*\)$/.exec(t)))     return decimalCodec(9, Number(m[1]));
    if ((m = /^Decimal64\(\s*(\d+)\s*\)$/.exec(t)))     return decimalCodec(18, Number(m[1]));
    if ((m = /^Decimal128\(\s*(\d+)\s*\)$/.exec(t)))    return decimalCodec(38, Number(m[1]));
    if ((m = /^Decimal256\(\s*(\d+)\s*\)$/.exec(t)))    return decimalCodec(76, Number(m[1]));
    if (/^Enum8\(/.test(t))                             return enumCodec(1);
    if (/^Enum16\(/.test(t))                            return enumCodec(2);

    throw new Error(`codecFor: unsupported type "${type}" (this draft covers plain types only)`);
}

// ---------------------------------------------------------------------------
// Row helpers
// ---------------------------------------------------------------------------

function encodeRow(types, values) {
    if (types.length !== values.length) throw new Error('encodeRow: types/values length mismatch');
    const w = new Writer();
    for (let i = 0; i < types.length; i++) {
        codecFor(types[i]).write(w, values[i]);
    }
    return w.bytes();
}

function decodeRow(types, bytes) {
    const r = new Reader(bytes);
    const out = new Array(types.length);
    for (let i = 0; i < types.length; i++) {
        out[i] = codecFor(types[i]).read(r);
    }
    return out;
}

function encodeRows(types, rows) {
    const w = new Writer();
    const codecs = types.map(codecFor);
    for (const row of rows) {
        if (row.length !== types.length) throw new Error('encodeRows: row width mismatch');
        for (let i = 0; i < codecs.length; i++) codecs[i].write(w, row[i]);
    }
    return w.bytes();
}

function decodeRows(types, bytes, rowCount) {
    const r = new Reader(bytes);
    const codecs = types.map(codecFor);
    const out = [];
    for (let i = 0; i < rowCount; i++) {
        const row = new Array(types.length);
        for (let j = 0; j < codecs.length; j++) row[j] = codecs[j].read(r);
        out.push(row);
    }
    return out;
}

// ---------------------------------------------------------------------------
// UUID, IPv6, Date helpers
// ---------------------------------------------------------------------------

function uuidFromString(s) {
    const hex = String(s).replace(/-/g, '');
    if (hex.length !== 32 || !/^[0-9a-fA-F]+$/.test(hex)) throw new RangeError(`invalid UUID: ${s}`);
    return BigInt('0x' + hex);
}

function uuidToString(big) {
    let h = big.toString(16).padStart(32, '0');
    return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}

function daysSinceEpoch(v) {
    const d = v instanceof Date ? v : new Date(v);
    return Math.floor(d.getTime() / 86400000);
}

function unixSeconds(v) {
    const d = v instanceof Date ? v : new Date(v);
    return Math.floor(d.getTime() / 1000);
}

function ipv6BytesToString(b) {
    // Render as 8 colon-separated groups of 4 hex digits (no :: collapsing —
    // keep the draft simple; downstream can canonicalise).
    const parts = [];
    for (let i = 0; i < 16; i += 2) {
        parts.push(((b[i] << 8) | b[i + 1]).toString(16).padStart(4, '0'));
    }
    return parts.join(':');
}

function ipv6StringToBytes(s) {
    // Accept either 8 colon-separated hex groups or "::"-compressed form.
    const str = String(s);
    let head, tail;
    if (str.includes('::')) {
        const [h, t] = str.split('::');
        head = h ? h.split(':') : [];
        tail = t ? t.split(':') : [];
        const fill = 8 - head.length - tail.length;
        if (fill < 0) throw new RangeError(`invalid IPv6: ${s}`);
        head = head.concat(Array(fill).fill('0')).concat(tail);
    } else {
        head = str.split(':');
    }
    if (head.length !== 8) throw new RangeError(`invalid IPv6: ${s}`);
    const out = new Uint8Array(16);
    for (let i = 0; i < 8; i++) {
        const v = parseInt(head[i], 16);
        if (!(v >= 0 && v <= 0xffff)) throw new RangeError(`invalid IPv6 group: ${head[i]}`);
        out[i * 2] = (v >> 8) & 0xff;
        out[i * 2 + 1] = v & 0xff;
    }
    return out;
}

// ---------------------------------------------------------------------------
// Exports
// ---------------------------------------------------------------------------

const RowBinary = {
    Reader,
    Writer,
    codecFor,
    encodeRow,
    decodeRow,
    encodeRows,
    decodeRows,
};

if (typeof module !== 'undefined' && module.exports) {
    module.exports = RowBinary;
}

// ---------------------------------------------------------------------------
// Self-test (run with `node docs/rowbinary.js`)
// ---------------------------------------------------------------------------

function _selfTest() {
    const cases = [
        // type, value, equality predicate (optional)
        ['Int8', -7],
        ['UInt8', 200],
        ['Int16', -30000],
        ['UInt16', 65535],
        ['Int32', -2147483648],
        ['UInt32', 4294967295],
        ['Int64', -9007199254740993n, (a, b) => a === b],
        ['UInt64', 18446744073709551614n, (a, b) => a === b],
        ['Int128', -(1n << 100n), (a, b) => a === b],
        ['UInt256', (1n << 200n) + 17n, (a, b) => a === b],
        ['Float32', 0.5],
        ['Float64', 3.141592653589793],
        ['Bool', true],
        ['Bool', false],
        ['String', 'hello, мир 🌍'],
        ['String', ''],
        ['FixedString(5)', 'hi', (a, b) => a[0] === 0x68 && a[1] === 0x69 && a[2] === 0 && a[3] === 0 && a[4] === 0],
        ['UUID', '61f0c404-5cb3-11e7-907b-a6006ad3dba0'],
        ['Date', new Date(Date.UTC(2024, 0, 15)), (a, b) => a.getTime() === b.getTime()],
        ['Date32', new Date(Date.UTC(1925, 5, 1)), (a, b) => a.getTime() === b.getTime()],
        ['DateTime', new Date(Date.UTC(2030, 11, 31, 23, 59, 58)), (a, b) => a.getTime() === b.getTime()],
        ["DateTime('UTC')", new Date(Date.UTC(2000, 0, 1)), (a, b) => a.getTime() === b.getTime()],
        ['DateTime64(3)', 1715000000123n, (a, b) => a === b],
        ['DateTime64(9, \'UTC\')', -1234567890123456789n, (a, b) => a === b],
        ['Time', 86399],
        ['Time64(6)', 86399_999_999n, (a, b) => a === b],
        ['Decimal(9,2)', 12345n, (a, b) => a === b],   // 123.45
        ['Decimal(18,4)', -9999999999n, (a, b) => a === b],
        ['Decimal(38,10)', (1n << 100n), (a, b) => a === b],
        ['Decimal(76,0)', -(1n << 250n), (a, b) => a === b],
        ['Enum8(\'a\'=1,\'b\'=2)', 2],
        ['Enum16(\'x\'=1000)', 1000],
        ['IPv4', '192.168.1.42'],
        ['IPv6', '2001:0db8:0000:0000:0000:ff00:0042:8329'],
    ];

    let pass = 0;
    let fail = 0;
    for (const [type, value, eq] of cases) {
        try {
            const codec = codecFor(type);
            const w = new Writer();
            codec.write(w, value);
            const decoded = codec.read(new Reader(w.bytes()));
            const ok = eq ? eq(decoded, value)
                          : (typeof value === 'object'
                                ? JSON.stringify(decoded) === JSON.stringify(value)
                                : Object.is(decoded, value));
            if (!ok) throw new Error(`mismatch: got ${repr(decoded)}, want ${repr(value)}`);
            pass++;
        } catch (err) {
            fail++;
            console.error(`FAIL  ${type}  value=${repr(value)}: ${err.message}`);
        }
    }

    // Multi-column row round-trip.
    try {
        const types  = ['Int32', 'String', 'Bool', 'DateTime'];
        const values = [42, 'row', true, new Date(Date.UTC(2025, 4, 9, 12, 0, 0))];
        const bytes  = encodeRow(types, values);
        const back   = decodeRow(types, bytes);
        if (back[0] !== 42 || back[1] !== 'row' || back[2] !== true || back[3].getTime() !== values[3].getTime()) {
            throw new Error(`row mismatch: ${repr(back)}`);
        }
        pass++;
    } catch (err) {
        fail++;
        console.error(`FAIL  multi-column row: ${err.message}`);
    }

    console.log(`rowbinary.js self-test: ${pass} passed, ${fail} failed`);
    if (fail > 0 && typeof process !== 'undefined') process.exit(1);
}

function repr(v) {
    if (typeof v === 'bigint') return v.toString() + 'n';
    if (v instanceof Uint8Array) return '[' + Array.from(v).join(',') + ']';
    if (v instanceof Date) return v.toISOString();
    return JSON.stringify(v);
}

if (typeof require !== 'undefined' && require.main === module) {
    _selfTest();
}
