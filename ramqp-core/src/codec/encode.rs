//! The [`Encode`] trait and its primitive implementations, plus the single-pass
//! composite/list/map/array encoders.
//!
//! Encoding never pre-computes a size by trial serialization. Compound headers
//! reserve a 4-byte length placeholder up front and **backpatch** it once the
//! body bytes are known — one pass over the data, never two.

use bytes::{BufMut, Bytes, BytesMut};
use ordered_float::OrderedFloat;
use uuid::Uuid;

use super::primitives::{OrderedMap, Symbol, codes};

/// A type that can be written to the AMQP 1.0 wire format.
///
/// Implementations write a complete constructor + value (a "typed" encoding),
/// so an `Option<T>` field encodes as an explicit `null` when `None`.
pub trait Encode {
    /// Append the typed encoding of `self` to `buf`.
    fn encode(&self, buf: &mut BytesMut);
}

/// Write a bare `null` (`0x40`).
pub fn encode_null(buf: &mut BytesMut) {
    buf.put_u8(codes::NULL);
}

fn encode_var(buf: &mut BytesMut, code8: u8, code32: u8, bytes: &[u8]) {
    if bytes.len() <= u8::MAX as usize {
        buf.put_u8(code8);
        buf.put_u8(bytes.len() as u8);
    } else {
        buf.put_u8(code32);
        buf.put_u32(bytes.len() as u32);
    }
    buf.put_slice(bytes);
}

// ---------------------------------------------------------------------------
// Primitive impls
// ---------------------------------------------------------------------------

impl Encode for bool {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(if *self {
            codes::BOOL_TRUE
        } else {
            codes::BOOL_FALSE
        });
    }
}

impl Encode for u8 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::UBYTE);
        buf.put_u8(*self);
    }
}

impl Encode for u16 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::USHORT);
        buf.put_u16(*self);
    }
}

impl Encode for u32 {
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            0 => buf.put_u8(codes::UINT_0),
            n if n <= u8::MAX as u32 => {
                buf.put_u8(codes::SMALL_UINT);
                buf.put_u8(n as u8);
            }
            n => {
                buf.put_u8(codes::UINT);
                buf.put_u32(n);
            }
        }
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut BytesMut) {
        match *self {
            0 => buf.put_u8(codes::ULONG_0),
            n if n <= u8::MAX as u64 => {
                buf.put_u8(codes::SMALL_ULONG);
                buf.put_u8(n as u8);
            }
            n => {
                buf.put_u8(codes::ULONG);
                buf.put_u64(n);
            }
        }
    }
}

impl Encode for i8 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::BYTE);
        buf.put_i8(*self);
    }
}

impl Encode for i16 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::SHORT);
        buf.put_i16(*self);
    }
}

impl Encode for i32 {
    fn encode(&self, buf: &mut BytesMut) {
        if (i8::MIN as i32..=i8::MAX as i32).contains(self) {
            buf.put_u8(codes::SMALL_INT);
            buf.put_i8(*self as i8);
        } else {
            buf.put_u8(codes::INT);
            buf.put_i32(*self);
        }
    }
}

impl Encode for i64 {
    fn encode(&self, buf: &mut BytesMut) {
        if (i8::MIN as i64..=i8::MAX as i64).contains(self) {
            buf.put_u8(codes::SMALL_LONG);
            buf.put_i8(*self as i8);
        } else {
            buf.put_u8(codes::LONG);
            buf.put_i64(*self);
        }
    }
}

impl Encode for f32 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::FLOAT);
        buf.put_f32(*self);
    }
}

impl Encode for f64 {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::DOUBLE);
        buf.put_f64(*self);
    }
}

impl Encode for OrderedFloat<f32> {
    fn encode(&self, buf: &mut BytesMut) {
        self.0.encode(buf);
    }
}

impl Encode for OrderedFloat<f64> {
    fn encode(&self, buf: &mut BytesMut) {
        self.0.encode(buf);
    }
}

impl Encode for char {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::CHAR);
        buf.put_u32(*self as u32);
    }
}

impl Encode for Uuid {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::UUID);
        buf.put_slice(self.as_bytes());
    }
}

impl Encode for str {
    fn encode(&self, buf: &mut BytesMut) {
        encode_var(buf, codes::STR8, codes::STR32, self.as_bytes());
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut BytesMut) {
        self.as_str().encode(buf);
    }
}

impl Encode for Symbol {
    fn encode(&self, buf: &mut BytesMut) {
        encode_var(buf, codes::SYM8, codes::SYM32, self.0.as_bytes());
    }
}

impl Encode for [u8] {
    fn encode(&self, buf: &mut BytesMut) {
        encode_var(buf, codes::VBIN8, codes::VBIN32, self);
    }
}

impl Encode for Bytes {
    fn encode(&self, buf: &mut BytesMut) {
        encode_var(buf, codes::VBIN8, codes::VBIN32, self);
    }
}

impl<T: Encode + ?Sized> Encode for &T {
    fn encode(&self, buf: &mut BytesMut) {
        (**self).encode(buf);
    }
}

impl<T: Encode + ?Sized> Encode for Box<T> {
    fn encode(&self, buf: &mut BytesMut) {
        (**self).encode(buf);
    }
}

impl<T: Encode> Encode for Option<T> {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Some(v) => v.encode(buf),
            None => encode_null(buf),
        }
    }
}

impl<K: Encode, V: Encode> Encode for OrderedMap<K, V> {
    fn encode(&self, buf: &mut BytesMut) {
        encode_map_entries(buf, self.iter());
    }
}

// ---------------------------------------------------------------------------
// Compound encoders
// ---------------------------------------------------------------------------

/// Write a descriptor as a `smallulong` when it fits, otherwise a `ulong`.
pub fn encode_descriptor(buf: &mut BytesMut, code: u64) {
    if code <= u8::MAX as u64 {
        buf.put_u8(codes::SMALL_ULONG);
        buf.put_u8(code as u8);
    } else {
        buf.put_u8(codes::ULONG);
        buf.put_u64(code);
    }
}

/// Records the start offset of each encoded field so [`encode_described_list`]
/// can elide trailing `null`s in a single pass.
#[derive(Debug)]
pub struct FieldWriter<'a> {
    buf: &'a mut BytesMut,
    starts: Vec<usize>,
}

impl FieldWriter<'_> {
    /// Encode one field.
    pub fn field<E: Encode + ?Sized>(&mut self, value: &E) {
        self.starts.push(self.buf.len());
        value.encode(self.buf);
    }

    /// Encode an explicit `null` field.
    pub fn null(&mut self) {
        self.starts.push(self.buf.len());
        encode_null(self.buf);
    }

    /// Encode a `multiple` symbol field: omitted when empty, an array of symbol
    /// otherwise (always the 32-bit element width so the array is homogeneous).
    pub fn symbols(&mut self, syms: &[Symbol]) {
        self.starts.push(self.buf.len());
        if syms.is_empty() {
            encode_null(self.buf);
        } else {
            encode_symbol_array(self.buf, syms);
        }
    }

    /// Encode a mandatory `multiple` symbol field: always present as an array
    /// (never elided to `null`, even when empty).
    pub fn symbols_required(&mut self, syms: &[Symbol]) {
        self.starts.push(self.buf.len());
        encode_symbol_array(self.buf, syms);
    }
}

/// Encode a described list (the composite-type workhorse): `0x00`, descriptor,
/// then a `list32` of the fields with trailing `null`s elided.
pub fn encode_described_list(
    buf: &mut BytesMut,
    descriptor: u64,
    fields: impl FnOnce(&mut FieldWriter),
) {
    buf.put_u8(codes::DESCRIBED);
    encode_descriptor(buf, descriptor);
    buf.put_u8(codes::LIST32);
    let size_pos = buf.len();
    buf.put_u32(0);
    let count_pos = buf.len();
    buf.put_u32(0);
    let content_start = buf.len();

    let mut fw = FieldWriter {
        buf,
        starts: Vec::new(),
    };
    fields(&mut fw);
    let FieldWriter { buf, starts } = fw;

    // Elide trailing fields encoded as a bare `null` (single 0x40 byte).
    let total = starts.len();
    let mut count = total;
    while count > 0 {
        let start = starts[count - 1];
        let end = if count < total {
            starts[count]
        } else {
            buf.len()
        };
        if end - start == 1 && buf[start] == codes::NULL {
            count -= 1;
        } else {
            break;
        }
    }
    let new_len = if count == total {
        buf.len()
    } else if count == 0 {
        content_start
    } else {
        starts[count]
    };
    buf.truncate(new_len);

    let size = (buf.len() - content_start + 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
    buf[count_pos..count_pos + 4].copy_from_slice(&(count as u32).to_be_bytes());
}

/// Encode a `map32` from an iterator of borrowed key/value pairs.
pub fn encode_map_entries<'a, K, V, I>(buf: &mut BytesMut, entries: I)
where
    K: Encode + 'a,
    V: Encode + 'a,
    I: IntoIterator<Item = (&'a K, &'a V)>,
{
    buf.put_u8(codes::MAP32);
    let size_pos = buf.len();
    buf.put_u32(0);
    let count_pos = buf.len();
    buf.put_u32(0);
    let content_start = buf.len();

    let mut count = 0u32;
    for (k, v) in entries {
        k.encode(buf);
        v.encode(buf);
        count += 2;
    }

    let size = (buf.len() - content_start + 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
    buf[count_pos..count_pos + 4].copy_from_slice(&count.to_be_bytes());
}

/// Encode a homogeneous `array32` of symbols (shared `sym32` constructor).
pub fn encode_symbol_array(buf: &mut BytesMut, syms: &[Symbol]) {
    buf.put_u8(codes::ARRAY32);
    let size_pos = buf.len();
    buf.put_u32(0);
    let count_pos = buf.len();
    buf.put_u32(0);
    let content_start = buf.len();

    buf.put_u8(codes::SYM32); // shared element constructor
    for s in syms {
        buf.put_u32(s.0.len() as u32);
        buf.put_slice(s.0.as_bytes());
    }

    let size = (buf.len() - content_start + 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
    buf[count_pos..count_pos + 4].copy_from_slice(&(syms.len() as u32).to_be_bytes());
}

/// Reserve a `list32`/`map32`-style 4-byte length + count header, returning the
/// positions to backpatch. Used by [`crate::codec::value`] for dynamic arrays.
pub(crate) fn open_compound(buf: &mut BytesMut, code: u8) -> (usize, usize, usize) {
    buf.put_u8(code);
    let size_pos = buf.len();
    buf.put_u32(0);
    let count_pos = buf.len();
    buf.put_u32(0);
    let content_start = buf.len();
    (size_pos, count_pos, content_start)
}

/// Backpatch a header opened by [`open_compound`].
pub(crate) fn close_compound(
    buf: &mut BytesMut,
    size_pos: usize,
    count_pos: usize,
    content_start: usize,
    count: u32,
) {
    let size = (buf.len() - content_start + 4) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
    buf[count_pos..count_pos + 4].copy_from_slice(&count.to_be_bytes());
}
