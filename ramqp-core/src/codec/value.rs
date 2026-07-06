//! The dynamic [`Value`] type: a fully self-describing AMQP value used wherever
//! the schema is open (map values, message annotations, filters, error info,
//! `amqp-value` bodies).

use bytes::{BufMut, Bytes, BytesMut};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use super::decode::{
    Decode, DecodeError, read_bytes, read_u8, read_u16, read_u32, read_u64, utf8_string,
};
use super::encode::{Encode, close_compound, encode_null, open_compound};
use super::primitives::{OrderedMap, Symbol, codes};

/// Any AMQP 1.0 value, decoded into an owned, self-describing tree.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[allow(missing_docs)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    Ubyte(u8),
    Ushort(u16),
    Uint(u32),
    Ulong(u64),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    Decimal32([u8; 4]),
    Decimal64([u8; 8]),
    Decimal128([u8; 16]),
    Char(char),
    /// Milliseconds since the Unix epoch.
    Timestamp(i64),
    Uuid(Uuid),
    Binary(Bytes),
    String(String),
    Symbol(Symbol),
    List(Vec<Value>),
    Map(OrderedMap<Value, Value>),
    Array(Vec<Value>),
    /// A described value: `(descriptor, body)`.
    Described(Box<Value>, Box<Value>),
}

impl Encode for Value {
    fn encode(&self, buf: &mut BytesMut) {
        use codes::*;
        match self {
            Value::Null => encode_null(buf),
            Value::Bool(v) => v.encode(buf),
            Value::Ubyte(v) => v.encode(buf),
            Value::Ushort(v) => v.encode(buf),
            Value::Uint(v) => v.encode(buf),
            Value::Ulong(v) => v.encode(buf),
            Value::Byte(v) => v.encode(buf),
            Value::Short(v) => v.encode(buf),
            Value::Int(v) => v.encode(buf),
            Value::Long(v) => v.encode(buf),
            Value::Float(v) => v.encode(buf),
            Value::Double(v) => v.encode(buf),
            Value::Decimal32(a) => {
                buf.put_u8(DECIMAL32);
                buf.put_slice(a);
            }
            Value::Decimal64(a) => {
                buf.put_u8(DECIMAL64);
                buf.put_slice(a);
            }
            Value::Decimal128(a) => {
                buf.put_u8(DECIMAL128);
                buf.put_slice(a);
            }
            Value::Char(v) => v.encode(buf),
            Value::Timestamp(ms) => {
                buf.put_u8(TIMESTAMP);
                buf.put_i64(*ms);
            }
            Value::Uuid(v) => v.encode(buf),
            Value::Binary(v) => v.encode(buf),
            Value::String(v) => v.encode(buf),
            Value::Symbol(v) => v.encode(buf),
            Value::List(items) => encode_value_list(buf, items),
            Value::Map(m) => m.encode(buf),
            Value::Array(items) => encode_value_array(buf, items),
            Value::Described(d, v) => {
                buf.put_u8(DESCRIBED);
                d.encode(buf);
                v.encode(buf);
            }
        }
    }
}

fn encode_value_list(buf: &mut BytesMut, items: &[Value]) {
    let (s, c, start) = open_compound(buf, codes::LIST32);
    for it in items {
        it.encode(buf);
    }
    close_compound(buf, s, c, start, items.len() as u32);
}

fn array_emit(buf: &mut BytesMut, ctor: u8, items: &[Value], f: impl Fn(&Value, &mut BytesMut)) {
    let (s, c, start) = open_compound(buf, codes::ARRAY32);
    buf.put_u8(ctor);
    for it in items {
        f(it, buf);
    }
    close_compound(buf, s, c, start, items.len() as u32);
}

fn encode_value_array(buf: &mut BytesMut, items: &[Value]) {
    use codes::*;
    if items.is_empty() {
        let (s, c, start) = open_compound(buf, ARRAY32);
        buf.put_u8(NULL);
        close_compound(buf, s, c, start, 0);
        return;
    }
    // Arrays are homogeneous; switch on the first element and emit every element
    // with the same wide constructor so the shared constructor is unambiguous.
    match &items[0] {
        Value::Symbol(_) => array_emit(buf, SYM32, items, |v, b| {
            if let Value::Symbol(s) = v {
                b.put_u32(s.0.len() as u32);
                b.put_slice(s.0.as_bytes());
            }
        }),
        Value::String(_) => array_emit(buf, STR32, items, |v, b| {
            if let Value::String(s) = v {
                b.put_u32(s.len() as u32);
                b.put_slice(s.as_bytes());
            }
        }),
        Value::Binary(_) => array_emit(buf, VBIN32, items, |v, b| {
            if let Value::Binary(x) = v {
                b.put_u32(x.len() as u32);
                b.put_slice(x);
            }
        }),
        Value::Ubyte(_) => array_emit(buf, UBYTE, items, |v, b| {
            if let Value::Ubyte(x) = v {
                b.put_u8(*x);
            }
        }),
        Value::Ushort(_) => array_emit(buf, USHORT, items, |v, b| {
            if let Value::Ushort(x) = v {
                b.put_u16(*x);
            }
        }),
        Value::Uint(_) => array_emit(buf, UINT, items, |v, b| {
            if let Value::Uint(x) = v {
                b.put_u32(*x);
            }
        }),
        Value::Ulong(_) => array_emit(buf, ULONG, items, |v, b| {
            if let Value::Ulong(x) = v {
                b.put_u64(*x);
            }
        }),
        Value::Int(_) => array_emit(buf, INT, items, |v, b| {
            if let Value::Int(x) = v {
                b.put_i32(*x);
            }
        }),
        Value::Long(_) => array_emit(buf, LONG, items, |v, b| {
            if let Value::Long(x) = v {
                b.put_i64(*x);
            }
        }),
        Value::Bool(_) => array_emit(buf, BOOL, items, |v, b| {
            if let Value::Bool(x) = v {
                b.put_u8(*x as u8);
            }
        }),
        Value::Float(_) => array_emit(buf, FLOAT, items, |v, b| {
            if let Value::Float(x) = v {
                b.put_f32(x.0);
            }
        }),
        Value::Double(_) => array_emit(buf, DOUBLE, items, |v, b| {
            if let Value::Double(x) = v {
                b.put_f64(x.0);
            }
        }),
        Value::Uuid(_) => array_emit(buf, UUID, items, |v, b| {
            if let Value::Uuid(x) = v {
                b.put_slice(x.as_bytes());
            }
        }),
        Value::Timestamp(_) => array_emit(buf, TIMESTAMP, items, |v, b| {
            if let Value::Timestamp(x) = v {
                b.put_i64(*x);
            }
        }),
        Value::Null => array_emit(buf, NULL, items, |_v, _b| {}),
        Value::Byte(_) => array_emit(buf, BYTE, items, |v, b| {
            if let Value::Byte(x) = v {
                b.put_i8(*x);
            }
        }),
        Value::Short(_) => array_emit(buf, SHORT, items, |v, b| {
            if let Value::Short(x) = v {
                b.put_i16(*x);
            }
        }),
        Value::Char(_) => array_emit(buf, CHAR, items, |v, b| {
            if let Value::Char(x) = v {
                b.put_u32(*x as u32);
            }
        }),
        Value::Decimal32(_) => array_emit(buf, DECIMAL32, items, |v, b| {
            if let Value::Decimal32(a) = v {
                b.put_slice(a);
            }
        }),
        Value::Decimal64(_) => array_emit(buf, DECIMAL64, items, |v, b| {
            if let Value::Decimal64(a) = v {
                b.put_slice(a);
            }
        }),
        Value::Decimal128(_) => array_emit(buf, DECIMAL128, items, |v, b| {
            if let Value::Decimal128(a) = v {
                b.put_slice(a);
            }
        }),
        // Compound elements share a single-byte constructor (our encoders always
        // emit the 32-bit forms), so strip each element's leading code byte.
        Value::List(_) | Value::Map(_) | Value::Array(_) => encode_compound_array(buf, items),
        // Described elements share `0x00 + descriptor + value-constructor`.
        Value::Described(_, _) => encode_described_array(buf, items),
    }
}

fn encode_compound_array(buf: &mut BytesMut, items: &[Value]) {
    // Reuse one scratch buffer across all elements (cleared between them) rather
    // than allocating a fresh BytesMut per element; the leading constructor byte
    // is shared by the array header, so only each element's body is copied in.
    let mut scratch = BytesMut::new();
    items[0].encode(&mut scratch);
    let ctor = scratch[0];
    let (s, c, start) = open_compound(buf, codes::ARRAY32);
    buf.put_u8(ctor);
    buf.put_slice(&scratch[1..]);
    for it in &items[1..] {
        scratch.clear();
        it.encode(&mut scratch);
        buf.put_slice(&scratch[1..]);
    }
    close_compound(buf, s, c, start, items.len() as u32);
}

fn encode_described_array(buf: &mut BytesMut, items: &[Value]) {
    let Value::Described(d0, v0) = &items[0] else {
        return encode_compound_array(buf, items);
    };
    let mut dbuf = BytesMut::new();
    d0.encode(&mut dbuf);
    let mut vbuf = BytesMut::new();
    v0.encode(&mut vbuf);
    let vctor = vbuf[0];
    let (s, c, start) = open_compound(buf, codes::ARRAY32);
    buf.put_u8(codes::DESCRIBED);
    buf.put_slice(&dbuf);
    buf.put_u8(vctor);
    buf.put_slice(&vbuf[1..]);
    // Reuse `vbuf` for the remaining element values instead of reallocating.
    for it in &items[1..] {
        if let Value::Described(_, v) = it {
            vbuf.clear();
            v.encode(&mut vbuf);
            buf.put_slice(&vbuf[1..]);
        }
    }
    close_compound(buf, s, c, start, items.len() as u32);
}

/// Maximum nesting depth accepted when decoding a self-describing [`Value`].
///
/// AMQP 1.0 sets no explicit bound on nesting, but a hostile or buggy peer can
/// encode a value that costs only a byte or two per level yet drives unbounded
/// recursion in the decoder — e.g. a run of `0x00` described-type constructors,
/// or deeply nested lists/maps/arrays. Without a cap, such a frame (bounded only
/// by `max-frame-size`) overflows the driver task's stack and crashes the
/// connection. The limit is chosen to stay comfortably within a 2 MiB worker
/// stack (each level costs a few stack frames) while sitting far above any
/// nesting a real application or broker produces.
const MAX_DEPTH: u32 = 64;

/// Backstop on the element count of an `array` whose elements are *zero-width*
/// (NULL, BOOL_TRUE/FALSE, UINT_0, ULONG_0, LIST_0). Such elements consume no
/// body bytes, so their count cannot be bounded by the remaining input the way
/// every other element type is. The array is necessarily degenerate — every
/// element is identical and carries no data — so a small ceiling is always
/// sufficient for legitimate data while preventing a few-byte frame from driving
/// up to `u32::MAX` allocations.
const MAX_ZERO_WIDTH_ARRAY: usize = 1024;

impl Decode for Value {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        decode_value(buf, 0)
    }
}

/// Read a 1-byte constructor and decode the value that follows at nesting `depth`.
fn decode_value(buf: &mut Bytes, depth: u32) -> Result<Value, DecodeError> {
    let code = read_u8(buf)?;
    decode_value_body(buf, code, depth)
}

fn utf8(b: Bytes, kind: &'static str) -> Result<String, DecodeError> {
    utf8_string(&b, kind)
}

fn fixed<const N: usize>(buf: &mut Bytes) -> Result<[u8; N], DecodeError> {
    let b = read_bytes(buf, N)?;
    let mut a = [0u8; N];
    a.copy_from_slice(&b);
    Ok(a)
}

/// Decode a value whose 1-byte constructor `code` has already been consumed,
/// rejecting input nested beyond [`MAX_DEPTH`] to bound decoder recursion.
fn decode_value_body(buf: &mut Bytes, code: u8, depth: u32) -> Result<Value, DecodeError> {
    use codes::*;
    if depth > MAX_DEPTH {
        return Err(DecodeError::NestingTooDeep { max: MAX_DEPTH });
    }
    Ok(match code {
        NULL => Value::Null,
        BOOL_TRUE => Value::Bool(true),
        BOOL_FALSE => Value::Bool(false),
        BOOL => Value::Bool(match read_u8(buf)? {
            0 => false,
            1 => true,
            n => {
                return Err(DecodeError::InvalidValue(format!(
                    "invalid boolean byte {n}"
                )));
            }
        }),
        UBYTE => Value::Ubyte(read_u8(buf)?),
        BYTE => Value::Byte(read_u8(buf)? as i8),
        USHORT => Value::Ushort(read_u16(buf)?),
        SHORT => Value::Short(read_u16(buf)? as i16),
        UINT_0 => Value::Uint(0),
        SMALL_UINT => Value::Uint(read_u8(buf)? as u32),
        UINT => Value::Uint(read_u32(buf)?),
        ULONG_0 => Value::Ulong(0),
        SMALL_ULONG => Value::Ulong(read_u8(buf)? as u64),
        ULONG => Value::Ulong(read_u64(buf)?),
        SMALL_INT => Value::Int(read_u8(buf)? as i8 as i32),
        INT => Value::Int(read_u32(buf)? as i32),
        SMALL_LONG => Value::Long(read_u8(buf)? as i8 as i64),
        LONG => Value::Long(read_u64(buf)? as i64),
        FLOAT => Value::Float(OrderedFloat(f32::from_bits(read_u32(buf)?))),
        DOUBLE => Value::Double(OrderedFloat(f64::from_bits(read_u64(buf)?))),
        DECIMAL32 => Value::Decimal32(fixed::<4>(buf)?),
        DECIMAL64 => Value::Decimal64(fixed::<8>(buf)?),
        DECIMAL128 => Value::Decimal128(fixed::<16>(buf)?),
        CHAR => Value::Char(
            char::from_u32(read_u32(buf)?)
                .ok_or_else(|| DecodeError::InvalidValue("invalid char code point".into()))?,
        ),
        TIMESTAMP => Value::Timestamp(read_u64(buf)? as i64),
        UUID => Value::Uuid(Uuid::from_slice(&read_bytes(buf, 16)?).expect("16 bytes")),
        VBIN8 => {
            let n = read_u8(buf)? as usize;
            Value::Binary(read_bytes(buf, n)?)
        }
        VBIN32 => {
            let n = read_u32(buf)? as usize;
            Value::Binary(read_bytes(buf, n)?)
        }
        STR8 => {
            let n = read_u8(buf)? as usize;
            Value::String(utf8(read_bytes(buf, n)?, "string")?)
        }
        STR32 => {
            let n = read_u32(buf)? as usize;
            Value::String(utf8(read_bytes(buf, n)?, "string")?)
        }
        SYM8 => {
            let n = read_u8(buf)? as usize;
            Value::Symbol(Symbol(utf8(read_bytes(buf, n)?, "symbol")?))
        }
        SYM32 => {
            let n = read_u32(buf)? as usize;
            Value::Symbol(Symbol(utf8(read_bytes(buf, n)?, "symbol")?))
        }
        LIST_0 => Value::List(Vec::new()),
        LIST8 => {
            let size = read_u8(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u8(&mut b)? as u32;
            Value::List(decode_n(&mut b, count, depth + 1)?)
        }
        LIST32 => {
            let size = read_u32(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u32(&mut b)?;
            Value::List(decode_n(&mut b, count, depth + 1)?)
        }
        MAP8 => {
            let size = read_u8(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u8(&mut b)? as u32;
            Value::Map(decode_map(&mut b, count, depth + 1)?)
        }
        MAP32 => {
            let size = read_u32(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u32(&mut b)?;
            Value::Map(decode_map(&mut b, count, depth + 1)?)
        }
        ARRAY8 => {
            let size = read_u8(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u8(&mut b)? as u32;
            Value::Array(decode_array(&mut b, count, depth + 1)?)
        }
        ARRAY32 => {
            let size = read_u32(buf)? as usize;
            let mut b = read_bytes(buf, size)?;
            let count = read_u32(&mut b)?;
            Value::Array(decode_array(&mut b, count, depth + 1)?)
        }
        DESCRIBED => {
            let d = decode_value(buf, depth + 1)?;
            let v = decode_value(buf, depth + 1)?;
            Value::Described(Box::new(d), Box::new(v))
        }
        c => {
            return Err(DecodeError::InvalidFormatCode {
                code: c,
                expected: "any value",
            });
        }
    })
}

// Capacity hints are clamped to the remaining input so an attacker-controlled
// `count` cannot drive a huge pre-allocation (each element occupies ≥ 1 byte).
fn cap_hint(count: u32, remaining: usize) -> usize {
    (count as usize).min(remaining)
}

fn decode_n(buf: &mut Bytes, count: u32, depth: u32) -> Result<Vec<Value>, DecodeError> {
    let mut out = Vec::with_capacity(cap_hint(count, buf.len()));
    for _ in 0..count {
        out.push(decode_value(buf, depth)?);
    }
    Ok(out)
}

fn decode_map(
    buf: &mut Bytes,
    count: u32,
    depth: u32,
) -> Result<OrderedMap<Value, Value>, DecodeError> {
    if !count.is_multiple_of(2) {
        return Err(DecodeError::InvalidValue(format!(
            "map has an odd element count {count}"
        )));
    }
    let entries = (count / 2) as usize;
    let mut map = OrderedMap::with_capacity(entries.min(buf.len()));
    for _ in 0..entries {
        let k = decode_value(buf, depth)?;
        let v = decode_value(buf, depth)?;
        map.push(k, v);
    }
    Ok(map)
}

fn decode_array(buf: &mut Bytes, count: u32, depth: u32) -> Result<Vec<Value>, DecodeError> {
    // The element constructor is always present, even for a zero-count array.
    let ctor = read_u8(buf)?;
    if ctor == codes::DESCRIBED {
        // A described array shares one descriptor and one element constructor,
        // followed by `count` bare element bodies. The shared descriptor is
        // cloned into every element but the last, which takes it by move.
        let descriptor = decode_value(buf, depth)?;
        let elem_ctor = read_u8(buf)?;
        check_array_count(count, elem_ctor, buf.len())?;
        let mut out = Vec::with_capacity(cap_hint(count, buf.len()));
        let mut descriptor = Some(descriptor);
        for i in 0..count {
            let body = decode_value_body(buf, elem_ctor, depth)?;
            let d = if i + 1 == count {
                descriptor
                    .take()
                    .expect("descriptor present until last element")
            } else {
                descriptor.as_ref().expect("descriptor present").clone()
            };
            out.push(Value::Described(Box::new(d), Box::new(body)));
        }
        Ok(out)
    } else {
        check_array_count(count, ctor, buf.len())?;
        let mut out = Vec::with_capacity(cap_hint(count, buf.len()));
        for _ in 0..count {
            out.push(decode_value_body(buf, ctor, depth)?);
        }
        Ok(out)
    }
}

/// Reject a hostile array `count` before it can drive unbounded allocation.
///
/// Array elements share a single constructor with no per-element framing. For a
/// zero-width element type each element consumes no body bytes, so `count` is
/// otherwise unbounded by the input (the OOM vector); for every other element
/// type each element consumes ≥ 1 byte, so a legitimate `count` cannot exceed
/// the remaining body. See [`MAX_ZERO_WIDTH_ARRAY`].
fn check_array_count(count: u32, elem_ctor: u8, remaining: usize) -> Result<(), DecodeError> {
    use codes::*;
    let zero_width = matches!(
        elem_ctor,
        NULL | BOOL_TRUE | BOOL_FALSE | UINT_0 | ULONG_0 | LIST_0
    );
    let limit = if zero_width {
        MAX_ZERO_WIDTH_ARRAY
    } else {
        remaining
    };
    if count as usize > limit {
        return Err(DecodeError::InvalidValue(format!(
            "array element count {count} exceeds limit {limit} \
             (element constructor {elem_ctor:#04x}, {remaining} body bytes)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_slice, to_vec};

    fn round_trip(v: Value) {
        let bytes = to_vec(&v);
        let back: Value = from_slice(&bytes).expect("decode");
        assert_eq!(v, back, "round-trip mismatch for {v:?}");
    }

    #[test]
    fn scalars_round_trip() {
        round_trip(Value::Null);
        round_trip(Value::Bool(true));
        round_trip(Value::Bool(false));
        round_trip(Value::Ubyte(0xab));
        round_trip(Value::Ushort(0xbeef));
        round_trip(Value::Uint(0));
        round_trip(Value::Uint(200));
        round_trip(Value::Uint(70_000));
        round_trip(Value::Ulong(0));
        round_trip(Value::Ulong(42));
        round_trip(Value::Ulong(1 << 40));
        round_trip(Value::Byte(-5));
        round_trip(Value::Short(-300));
        round_trip(Value::Int(-1));
        round_trip(Value::Int(100_000));
        round_trip(Value::Long(-1));
        round_trip(Value::Long(-1 << 40));
        round_trip(Value::Float(OrderedFloat(1.5)));
        round_trip(Value::Double(OrderedFloat(-2.25)));
        round_trip(Value::Char('Z'));
        round_trip(Value::Char('🦀'));
        round_trip(Value::Timestamp(1_700_000_000_000));
        round_trip(Value::Uuid(Uuid::from_u128(
            0x1234_5678_9abc_def0_1122_3344_5566_7788,
        )));
        round_trip(Value::Binary(Bytes::from_static(b"\x00\x01\x02hello")));
        round_trip(Value::String("héllo".into()));
        round_trip(Value::Symbol(Symbol::new("amqp:accepted:list")));
    }

    #[test]
    fn small_int_boundaries() {
        // Values at the small/large encoding boundary must round-trip.
        for v in [0i32, 127, 128, -128, -129, i32::MAX, i32::MIN] {
            round_trip(Value::Int(v));
        }
        for v in [0u32, 255, 256, u32::MAX] {
            round_trip(Value::Uint(v));
        }
    }

    #[test]
    fn compound_round_trip() {
        round_trip(Value::List(vec![]));
        round_trip(Value::List(vec![
            Value::Uint(1),
            Value::String("two".into()),
            Value::Null,
            Value::Bool(true),
        ]));
        let map = OrderedMap::from(vec![
            (Value::Symbol(Symbol::new("key")), Value::Uint(7)),
            (Value::String("k2".into()), Value::Null),
        ]);
        round_trip(Value::Map(map));
    }

    #[test]
    fn array_round_trip() {
        round_trip(Value::Array(vec![
            Value::Symbol(Symbol::new("A")),
            Value::Symbol(Symbol::new("BB")),
            Value::Symbol(Symbol::new("CCC")),
        ]));
        round_trip(Value::Array(vec![
            Value::Uint(1),
            Value::Uint(2),
            Value::Uint(3),
        ]));
        round_trip(Value::Array(vec![]));
    }

    #[test]
    fn array_of_all_element_kinds_round_trips_as_array() {
        // Previously these silently degraded to a list; now they must stay arrays.
        for v in [
            Value::Array(vec![Value::Byte(-1), Value::Byte(2)]),
            Value::Array(vec![Value::Short(-300), Value::Short(300)]),
            Value::Array(vec![Value::Char('a'), Value::Char('🦀')]),
            Value::Array(vec![Value::Bool(true), Value::Bool(false)]),
            Value::Array(vec![Value::Null, Value::Null]),
            Value::Array(vec![Value::Decimal32([1, 2, 3, 4])]),
            // compound element type (nested lists)
            Value::Array(vec![
                Value::List(vec![Value::Uint(1)]),
                Value::List(vec![Value::Uint(2), Value::Uint(3)]),
            ]),
        ] {
            let bytes = to_vec(&v);
            // first byte must be an array constructor, never a list
            assert!(
                bytes[0] == codes::ARRAY8 || bytes[0] == codes::ARRAY32,
                "expected array constructor for {v:?}, got {:#04x}",
                bytes[0]
            );
            round_trip(v);
        }
    }

    #[test]
    fn array_of_described_round_trips() {
        let v = Value::Array(vec![
            Value::Described(Box::new(Value::Ulong(0x24)), Box::new(Value::List(vec![]))),
            Value::Described(Box::new(Value::Ulong(0x24)), Box::new(Value::List(vec![]))),
        ]);
        round_trip(v);
    }

    #[test]
    fn strict_boolean_rejects_non_canonical() {
        // 0x56 (one-byte boolean) with a byte other than 0/1 is invalid.
        assert!(from_slice::<Value>(&[0x56, 0x02]).is_err());
        assert!(from_slice::<bool>(&[0x56, 0xff]).is_err());
        assert!(from_slice::<bool>(&[0x56, 0x01]).unwrap());
        assert!(!from_slice::<bool>(&[0x56, 0x00]).unwrap());
    }

    #[test]
    fn described_round_trip() {
        round_trip(Value::Described(
            Box::new(Value::Ulong(0x24)),
            Box::new(Value::List(vec![])),
        ));
    }

    #[test]
    fn deeply_nested_described_is_rejected_not_panicked() {
        // A long run of described-type constructors (0x00) costs one byte per
        // nesting level. Without the depth bound this overflows the stack; with
        // it we get a clean `NestingTooDeep` error instead of a crash.
        let bytes = vec![0x00u8; 100_000];
        let r: Result<Value, _> = from_slice(&bytes);
        assert!(
            matches!(r, Err(DecodeError::NestingTooDeep { .. })),
            "expected NestingTooDeep, got {r:?}"
        );
    }

    #[test]
    fn deeply_nested_lists_are_rejected_not_panicked() {
        // Build, inside-out, a chain of single-element LIST32s nested deeper than
        // MAX_DEPTH. Each level's size prefix correctly encloses the inner value,
        // so this is a genuine nesting (~9 bytes/level), and the depth bound must
        // reject it rather than overflowing the stack.
        let mut inner = vec![codes::NULL];
        for _ in 0..(MAX_DEPTH as usize + 50) {
            let size = (4 + inner.len()) as u32; // 4-byte count + body
            let mut lvl = vec![codes::LIST32];
            lvl.extend_from_slice(&size.to_be_bytes());
            lvl.extend_from_slice(&1u32.to_be_bytes()); // count = 1
            lvl.extend_from_slice(&inner);
            inner = lvl;
        }
        let r: Result<Value, _> = from_slice(&inner);
        assert!(
            matches!(r, Err(DecodeError::NestingTooDeep { .. })),
            "expected NestingTooDeep, got {r:?}"
        );
    }

    #[test]
    fn hostile_zero_width_array_count_is_rejected_not_ooming() {
        // ARRAY32 with count = u32::MAX and a zero-width element constructor
        // (NULL). The body is a handful of bytes, but without the count bound
        // each of ~4.3 billion elements would push a `Value` and OOM the
        // decoder. We must reject it cleanly instead.
        let mut bytes = vec![codes::ARRAY32];
        let size = 4u32 + 1; // 4-byte count + 1-byte element constructor
        bytes.extend_from_slice(&size.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes()); // count
        bytes.push(codes::NULL); // zero-width element constructor
        let r: Result<Value, _> = from_slice(&bytes);
        assert!(
            matches!(r, Err(DecodeError::InvalidValue(_))),
            "expected InvalidValue for hostile array count, got {r:?}"
        );
    }

    #[test]
    fn small_zero_width_array_still_decodes() {
        // A legitimate (degenerate) array of three nulls must still decode.
        let v = Value::Array(vec![Value::Null, Value::Null, Value::Null]);
        round_trip(v);
    }

    #[test]
    fn map_with_odd_element_count_is_rejected() {
        // MAP32 declaring an odd element count (3) is malformed — a map is
        // key/value pairs. Reject rather than silently dropping the dangling key.
        let mut body = Vec::new();
        body.extend_from_slice(&3u32.to_be_bytes()); // count = 3 (odd)
        body.push(codes::NULL);
        body.push(codes::NULL);
        body.push(codes::NULL);
        let mut bytes = vec![codes::MAP32];
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
        let r: Result<Value, _> = from_slice(&bytes);
        assert!(
            matches!(r, Err(DecodeError::InvalidValue(_))),
            "expected InvalidValue for odd map count, got {r:?}"
        );
    }

    #[test]
    fn truncated_input_errors_not_panics() {
        let bytes = to_vec(&Value::String("a long enough string value".into()));
        for cut in 0..bytes.len() {
            let r: Result<Value, _> = from_slice(&bytes[..cut]);
            assert!(r.is_err(), "expected error decoding truncated len {cut}");
        }
    }

    /// Exact wire bytes per the spec — guards against a self-consistent but
    /// wrong codec that would still pass round-trip tests.
    #[test]
    fn golden_scalar_vectors() {
        assert_eq!(to_vec(&Value::Null), [0x40]);
        assert_eq!(to_vec(&Value::Bool(true)), [0x41]);
        assert_eq!(to_vec(&Value::Bool(false)), [0x42]);
        assert_eq!(to_vec(&Value::Uint(0)), [0x43]);
        assert_eq!(to_vec(&Value::Uint(5)), [0x52, 0x05]);
        assert_eq!(
            to_vec(&Value::Uint(0x1_0000)),
            [0x70, 0x00, 0x01, 0x00, 0x00]
        );
        assert_eq!(to_vec(&Value::Ulong(0)), [0x44]);
        assert_eq!(to_vec(&Value::Ulong(0x10)), [0x53, 0x10]);
        assert_eq!(to_vec(&Value::Long(-1)), [0x55, 0xff]);
        assert_eq!(to_vec(&Value::Timestamp(0)), [0x83, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(to_vec(&Value::String("hello".into())), b"\xa1\x05hello");
        assert_eq!(to_vec(&Value::Symbol(Symbol::new("amqp"))), b"\xa3\x04amqp");
        assert_eq!(
            to_vec(&Value::Binary(Bytes::from_static(b"\x01\x02"))),
            [0xa0, 0x02, 0x01, 0x02]
        );
    }

    /// A described list with trailing `null` fields must elide them: descriptor
    /// `0x24` (accepted) carrying `[uint(7), null, null]` → one field.
    #[test]
    fn golden_described_list_elision() {
        use crate::codec::encode::encode_described_list;
        let mut buf = BytesMut::new();
        encode_described_list(&mut buf, 0x24, |fw| {
            fw.field(&Value::Uint(7));
            fw.null();
            fw.null();
        });
        // 0x00 0x53 0x24 | 0xd0 size=6 count=1 | 0x52 0x07
        assert_eq!(
            buf.to_vec(),
            [0x00, 0x53, 0x24, 0xd0, 0, 0, 0, 6, 0, 0, 0, 1, 0x52, 0x07]
        );
    }
}
