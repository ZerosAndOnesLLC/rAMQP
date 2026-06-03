//! The [`Decode`] trait, its primitive implementations, and the low-level
//! header readers.
//!
//! Decoding is zero-copy for byte runs: variable-width binaries, strings, and
//! symbols are carved out of the input with [`bytes::Bytes::split_to`], which
//! bumps a refcount rather than copying.

use bytes::{Buf, Bytes};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use super::primitives::{OrderedMap, Symbol, codes};

/// Errors produced while decoding the AMQP 1.0 wire format.
#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    /// Input ended before a value could be fully read.
    #[error("unexpected end of input: needed {needed} more byte(s)")]
    Eof {
        /// How many more bytes were required.
        needed: usize,
    },
    /// A format code was not valid where one was expected.
    #[error("invalid format code {code:#04x} (expected {expected})")]
    InvalidFormatCode {
        /// The offending code.
        code: u8,
        /// What kind of value was expected.
        expected: &'static str,
    },
    /// A byte run that should have been UTF-8 (string/symbol) was not.
    #[error("invalid UTF-8 in {kind}")]
    InvalidUtf8 {
        /// Which kind of value failed validation.
        kind: &'static str,
    },
    /// A value was structurally valid but semantically wrong.
    #[error("invalid value: {0}")]
    InvalidValue(String),
    /// A composite carried a descriptor other than the expected one.
    #[error("expected descriptor {expected:#x}, found {found}")]
    UnexpectedDescriptor {
        /// The descriptor the caller asked for.
        expected: u64,
        /// What was actually present.
        found: String,
    },
    /// A mandatory composite field was absent or `null`.
    #[error("missing mandatory field `{0}`")]
    MissingField(&'static str),
    /// A length prefix exceeded what the input could contain.
    #[error("length overflow")]
    Overflow,
}

/// A type that can be read from the AMQP 1.0 wire format.
pub trait Decode: Sized {
    /// Decode `Self` from the front of `buf`, advancing past the bytes consumed.
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError>;
}

// ---------------------------------------------------------------------------
// Low-level read helpers (bounds-checked)
// ---------------------------------------------------------------------------

pub(crate) fn ensure(buf: &Bytes, n: usize) -> Result<(), DecodeError> {
    if buf.len() < n {
        Err(DecodeError::Eof {
            needed: n - buf.len(),
        })
    } else {
        Ok(())
    }
}

pub(crate) fn read_u8(buf: &mut Bytes) -> Result<u8, DecodeError> {
    ensure(buf, 1)?;
    Ok(buf.get_u8())
}

pub(crate) fn read_u16(buf: &mut Bytes) -> Result<u16, DecodeError> {
    ensure(buf, 2)?;
    Ok(buf.get_u16())
}

pub(crate) fn read_u32(buf: &mut Bytes) -> Result<u32, DecodeError> {
    ensure(buf, 4)?;
    Ok(buf.get_u32())
}

pub(crate) fn read_u64(buf: &mut Bytes) -> Result<u64, DecodeError> {
    ensure(buf, 8)?;
    Ok(buf.get_u64())
}

pub(crate) fn read_bytes(buf: &mut Bytes, n: usize) -> Result<Bytes, DecodeError> {
    ensure(buf, n)?;
    Ok(buf.split_to(n))
}

/// Peek the next format code without consuming it.
pub fn peek_code(buf: &Bytes) -> Option<u8> {
    buf.first().copied()
}

/// Consume and discard a `null` if one is at the cursor; returns whether it was.
fn take_null(buf: &mut Bytes) -> bool {
    if peek_code(buf) == Some(codes::NULL) {
        buf.advance(1);
        true
    } else {
        false
    }
}

fn bad(code: u8, expected: &'static str) -> DecodeError {
    DecodeError::InvalidFormatCode { code, expected }
}

// ---------------------------------------------------------------------------
// Primitive impls
// ---------------------------------------------------------------------------

impl Decode for bool {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::BOOL_TRUE => Ok(true),
            codes::BOOL_FALSE => Ok(false),
            codes::BOOL => match read_u8(buf)? {
                0 => Ok(false),
                1 => Ok(true),
                n => Err(DecodeError::InvalidValue(format!(
                    "invalid boolean byte {n}"
                ))),
            },
            c => Err(bad(c, "boolean")),
        }
    }
}

impl Decode for u8 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::UBYTE => read_u8(buf),
            c => Err(bad(c, "ubyte")),
        }
    }
}

impl Decode for u16 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::USHORT => read_u16(buf),
            c => Err(bad(c, "ushort")),
        }
    }
}

impl Decode for u32 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::UINT_0 => Ok(0),
            codes::SMALL_UINT => Ok(read_u8(buf)? as u32),
            codes::UINT => read_u32(buf),
            c => Err(bad(c, "uint")),
        }
    }
}

impl Decode for u64 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::ULONG_0 => Ok(0),
            codes::SMALL_ULONG => Ok(read_u8(buf)? as u64),
            codes::ULONG => read_u64(buf),
            c => Err(bad(c, "ulong")),
        }
    }
}

impl Decode for i8 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::BYTE => Ok(read_u8(buf)? as i8),
            c => Err(bad(c, "byte")),
        }
    }
}

impl Decode for i16 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::SHORT => Ok(read_u16(buf)? as i16),
            c => Err(bad(c, "short")),
        }
    }
}

impl Decode for i32 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::SMALL_INT => Ok(read_u8(buf)? as i8 as i32),
            codes::INT => Ok(read_u32(buf)? as i32),
            c => Err(bad(c, "int")),
        }
    }
}

impl Decode for i64 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::SMALL_LONG => Ok(read_u8(buf)? as i8 as i64),
            codes::LONG => Ok(read_u64(buf)? as i64),
            c => Err(bad(c, "long")),
        }
    }
}

impl Decode for f32 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::FLOAT => Ok(f32::from_bits(read_u32(buf)?)),
            c => Err(bad(c, "float")),
        }
    }
}

impl Decode for f64 {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::DOUBLE => Ok(f64::from_bits(read_u64(buf)?)),
            c => Err(bad(c, "double")),
        }
    }
}

impl Decode for OrderedFloat<f32> {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(OrderedFloat(f32::decode(buf)?))
    }
}

impl Decode for OrderedFloat<f64> {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(OrderedFloat(f64::decode(buf)?))
    }
}

impl Decode for char {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::CHAR => char::from_u32(read_u32(buf)?)
                .ok_or_else(|| DecodeError::InvalidValue("invalid char code point".into())),
            c => Err(bad(c, "char")),
        }
    }
}

impl Decode for Uuid {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match read_u8(buf)? {
            codes::UUID => {
                let raw = read_bytes(buf, 16)?;
                Ok(Uuid::from_slice(&raw).expect("16 bytes is a valid uuid"))
            }
            c => Err(bad(c, "uuid")),
        }
    }
}

impl Decode for String {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        let len = match read_u8(buf)? {
            codes::STR8 => read_u8(buf)? as usize,
            codes::STR32 => read_u32(buf)? as usize,
            c => return Err(bad(c, "string")),
        };
        let raw = read_bytes(buf, len)?;
        String::from_utf8(raw.to_vec()).map_err(|_| DecodeError::InvalidUtf8 { kind: "string" })
    }
}

impl Decode for Symbol {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        let len = match read_u8(buf)? {
            codes::SYM8 => read_u8(buf)? as usize,
            codes::SYM32 => read_u32(buf)? as usize,
            c => return Err(bad(c, "symbol")),
        };
        let raw = read_bytes(buf, len)?;
        let s = String::from_utf8(raw.to_vec())
            .map_err(|_| DecodeError::InvalidUtf8 { kind: "symbol" })?;
        Ok(Symbol(s))
    }
}

impl Decode for Bytes {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        let len = match read_u8(buf)? {
            codes::VBIN8 => read_u8(buf)? as usize,
            codes::VBIN32 => read_u32(buf)? as usize,
            c => return Err(bad(c, "binary")),
        };
        read_bytes(buf, len)
    }
}

impl<T: Decode> Decode for Box<T> {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(Box::new(T::decode(buf)?))
    }
}

impl<T: Decode> Decode for Option<T> {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        if take_null(buf) {
            Ok(None)
        } else {
            Ok(Some(T::decode(buf)?))
        }
    }
}

impl<K: Decode, V: Decode> Decode for OrderedMap<K, V> {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        let (count, mut body) = read_map_header(buf)?;
        let entries = (count / 2) as usize;
        // Clamp the capacity hint to the input so a hostile count cannot
        // pre-allocate unbounded memory (each entry needs ≥ 2 bytes).
        let mut map = OrderedMap::with_capacity(entries.min(body.len()));
        for _ in 0..entries {
            let k = K::decode(&mut body)?;
            let v = V::decode(&mut body)?;
            map.push(k, v);
        }
        Ok(map)
    }
}

// ---------------------------------------------------------------------------
// Compound header readers
// ---------------------------------------------------------------------------

/// Read a list header, returning `(count, elements)` where `elements` is a
/// zero-copy slice of just the element bytes.
pub fn read_list_header(buf: &mut Bytes) -> Result<(u32, Bytes), DecodeError> {
    match read_u8(buf)? {
        codes::LIST_0 => Ok((0, Bytes::new())),
        codes::LIST8 => {
            let size = read_u8(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u8(&mut body)? as u32;
            Ok((count, body))
        }
        codes::LIST32 => {
            let size = read_u32(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u32(&mut body)?;
            Ok((count, body))
        }
        c => Err(bad(c, "list")),
    }
}

/// Read a map header, returning `(element_count, elements)`; the element count
/// is twice the number of key/value entries.
pub fn read_map_header(buf: &mut Bytes) -> Result<(u32, Bytes), DecodeError> {
    match read_u8(buf)? {
        codes::MAP8 => {
            let size = read_u8(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u8(&mut body)? as u32;
            Ok((count, body))
        }
        codes::MAP32 => {
            let size = read_u32(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u32(&mut body)?;
            Ok((count, body))
        }
        c => Err(bad(c, "map")),
    }
}

/// Read an array header, returning `(count, body)` where `body` begins with the
/// shared element constructor followed by the bare element bodies.
pub fn read_array_header(buf: &mut Bytes) -> Result<(u32, Bytes), DecodeError> {
    match read_u8(buf)? {
        codes::ARRAY8 => {
            let size = read_u8(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u8(&mut body)? as u32;
            Ok((count, body))
        }
        codes::ARRAY32 => {
            let size = read_u32(buf)? as usize;
            let mut body = read_bytes(buf, size)?;
            let count = read_u32(&mut body)?;
            Ok((count, body))
        }
        c => Err(bad(c, "array")),
    }
}
