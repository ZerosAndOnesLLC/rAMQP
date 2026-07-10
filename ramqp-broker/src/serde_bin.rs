//! `serde_bin` — the broker's own compact binary serialization format, an
//! in-house replacement for the unmaintained `bincode` crate (RUSTSEC-2025-0141,
//! tracked in issue #20).
//!
//! It implements the full serde data model using the same wire model bincode 1.x
//! produced by default, so it round-trips every type the cluster layer already
//! serialized through bincode — **including openraft's generic types**
//! (`Entry`, `Vote`, `SnapshotMeta`, `LogId`, `StoredMembership`):
//!
//! - integers: little-endian, fixed width (`i8..=i128`, `u8..=u128`);
//! - `bool`: one byte (`0`/`1`); `char`: a `u32`; floats: IEEE-754 bits, LE;
//! - `Option`: a one-byte tag (`0` = none, `1` = some) then the value;
//! - sequences, maps, strings, and byte strings: a `u64` length prefix then the
//!   elements/bytes;
//! - enum variants: a `u32` variant index then the variant's data;
//! - structs and tuples: their fields in declaration order, no framing.
//!
//! The format is **not self-describing** (there are no type tags on the wire),
//! so it cannot implement `deserialize_any` — exactly like bincode. Every type
//! we use has a shape known at both ends, so that costs us nothing. Both
//! directions are covered by the tests below and, end to end, by the cluster and
//! chaos suites (real Raft replication and snapshots ride this format).

use std::fmt;

use serde::Serialize;
use serde::de::{self, DeserializeOwned, DeserializeSeed, IntoDeserializer, Visitor};
use serde::ser;

// ---------------------------------------------------------------------------
// Public API — drop-in for the `bincode::{serialize, deserialize}` we replaced.
// ---------------------------------------------------------------------------

/// Serialize `value` into a fresh byte vector.
pub fn to_vec<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, Error> {
    let mut serializer = Serializer { out: Vec::new() };
    value.serialize(&mut serializer)?;
    Ok(serializer.out)
}

/// Deserialize a `T` from exactly `bytes`. Trailing bytes are an error — the
/// broker's framing always hands us an exact-length slice, so extra bytes mean a
/// corrupt or mismatched payload rather than a stream boundary.
pub fn from_slice<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let mut de = Deserializer { input: bytes };
    let value = T::deserialize(&mut de)?;
    if de.input.is_empty() {
        Ok(value)
    } else {
        Err(Error::TrailingBytes(de.input.len()))
    }
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// A serialization or deserialization failure.
#[derive(Debug)]
pub enum Error {
    /// A `serde`-reported error (custom message from a (De)serialize impl).
    Message(String),
    /// The input ended before a value was fully decoded.
    Eof,
    /// Bytes remained after the top-level value was decoded.
    TrailingBytes(usize),
    /// A boolean byte was neither 0 nor 1.
    InvalidBool(u8),
    /// A `char` value was not a valid Unicode scalar.
    InvalidChar(u32),
    /// A string field was not valid UTF-8.
    InvalidUtf8,
    /// An `Option` tag byte was neither 0 nor 1.
    InvalidOptionTag(u8),
    /// A sequence/map was serialized without a known length (unsupported).
    SequenceLengthRequired,
    /// A `u64` length prefix did not fit in `usize` on this platform.
    LengthOverflow(u64),
    /// `deserialize_any`/`deserialize_ignored_any` on a non-self-describing
    /// format — never needed by the types we use.
    NotSelfDescribing,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Message(m) => write!(f, "{m}"),
            Error::Eof => write!(f, "unexpected end of input"),
            Error::TrailingBytes(n) => write!(f, "{n} trailing byte(s) after value"),
            Error::InvalidBool(b) => write!(f, "invalid bool byte {b}"),
            Error::InvalidChar(c) => write!(f, "invalid char scalar {c:#x}"),
            Error::InvalidUtf8 => write!(f, "invalid UTF-8 in string"),
            Error::InvalidOptionTag(t) => write!(f, "invalid Option tag {t}"),
            Error::SequenceLengthRequired => write!(f, "sequence length must be known"),
            Error::LengthOverflow(n) => write!(f, "length {n} exceeds usize"),
            Error::NotSelfDescribing => {
                write!(f, "self-describing deserialization is not supported")
            }
        }
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

/// Writes the compact binary form into an owned buffer. Internal — the public
/// surface is [`to_vec`].
struct Serializer {
    out: Vec<u8>,
}

impl Serializer {
    #[inline]
    fn w(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }
    #[inline]
    fn write_len(&mut self, n: usize) {
        self.w(&(n as u64).to_le_bytes());
    }
    #[inline]
    fn write_u32(&mut self, n: u32) {
        self.w(&n.to_le_bytes());
    }
}

macro_rules! ser_num {
    ($($method:ident : $ty:ty,)*) => { $(
        fn $method(self, v: $ty) -> Result<(), Error> {
            self.w(&v.to_le_bytes());
            Ok(())
        }
    )* };
}

impl ser::Serializer for &mut Serializer {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    ser_num! {
        serialize_i8: i8, serialize_i16: i16, serialize_i32: i32,
        serialize_i64: i64, serialize_i128: i128,
        serialize_u8: u8, serialize_u16: u16, serialize_u32: u32,
        serialize_u64: u64, serialize_u128: u128,
    }

    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.out.push(u8::from(v));
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.w(&v.to_bits().to_le_bytes());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.w(&v.to_bits().to_le_bytes());
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.write_u32(v as u32);
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.write_len(v.len());
        self.w(v.as_bytes());
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.write_len(v.len());
        self.w(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.out.push(0);
        Ok(())
    }
    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<(), Error> {
        self.out.push(1);
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        Ok(())
    }
    fn serialize_unit_variant(
        self,
        _name: &'static str,
        index: u32,
        _variant: &'static str,
    ) -> Result<(), Error> {
        self.write_u32(index);
        Ok(())
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        index: u32,
        _variant: &'static str,
        v: &T,
    ) -> Result<(), Error> {
        self.write_u32(index);
        v.serialize(self)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self, Error> {
        let n = len.ok_or(Error::SequenceLengthRequired)?;
        self.write_len(n);
        Ok(self)
    }
    fn serialize_tuple(self, _len: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, Error> {
        self.write_u32(index);
        Ok(self)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self, Error> {
        let n = len.ok_or(Error::SequenceLengthRequired)?;
        self.write_len(n);
        Ok(self)
    }
    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self, Error> {
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self, Error> {
        self.write_u32(index);
        Ok(self)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}

// All the "compound" serialize traits just write their elements in order into
// the same buffer, so one impl per trait over `&mut Serializer` suffices.
macro_rules! ser_compound_elem {
    ($trait:ident, $method:ident) => {
        impl ser::$trait for &mut Serializer {
            type Ok = ();
            type Error = Error;
            fn $method<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Error> {
                value.serialize(&mut **self)
            }
            fn end(self) -> Result<(), Error> {
                Ok(())
            }
        }
    };
}
ser_compound_elem!(SerializeSeq, serialize_element);
ser_compound_elem!(SerializeTuple, serialize_element);
ser_compound_elem!(SerializeTupleStruct, serialize_field);
ser_compound_elem!(SerializeTupleVariant, serialize_field);

impl ser::SerializeStruct for &mut Serializer {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Serializer {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeMap for &mut Serializer {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Error> {
        key.serialize(&mut **self)
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deserializer
// ---------------------------------------------------------------------------

/// Reads the compact binary form from a byte slice. Internal — the public
/// surface is [`from_slice`].
struct Deserializer<'de> {
    input: &'de [u8],
}

impl<'de> Deserializer<'de> {
    #[inline]
    fn take(&mut self, n: usize) -> Result<&'de [u8], Error> {
        if self.input.len() < n {
            return Err(Error::Eof);
        }
        let (head, tail) = self.input.split_at(n);
        self.input = tail;
        Ok(head)
    }
    #[inline]
    fn read_u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }
    #[inline]
    fn read_u32(&mut self) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    #[inline]
    fn read_len(&mut self) -> Result<usize, Error> {
        let n = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
        usize::try_from(n).map_err(|_| Error::LengthOverflow(n))
    }
}

macro_rules! de_num {
    ($($method:ident : $ty:ty => $visit:ident, $n:expr,)*) => { $(
        fn $method<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
            let bytes = self.take($n)?;
            visitor.$visit(<$ty>::from_le_bytes(bytes.try_into().unwrap()))
        }
    )* };
}

impl<'de> de::Deserializer<'de> for &mut Deserializer<'de> {
    type Error = Error;

    de_num! {
        deserialize_i8: i8 => visit_i8, 1,
        deserialize_i16: i16 => visit_i16, 2,
        deserialize_i32: i32 => visit_i32, 4,
        deserialize_i64: i64 => visit_i64, 8,
        deserialize_i128: i128 => visit_i128, 16,
        deserialize_u8: u8 => visit_u8, 1,
        deserialize_u16: u16 => visit_u16, 2,
        deserialize_u32: u32 => visit_u32, 4,
        deserialize_u64: u64 => visit_u64, 8,
        deserialize_u128: u128 => visit_u128, 16,
    }

    fn deserialize_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::NotSelfDescribing)
    }
    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.read_u8()? {
            0 => visitor.visit_bool(false),
            1 => visitor.visit_bool(true),
            b => Err(Error::InvalidBool(b)),
        }
    }
    fn deserialize_f32<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let bits = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
        visitor.visit_f32(f32::from_bits(bits))
    }
    fn deserialize_f64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let bits = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
        visitor.visit_f64(f64::from_bits(bits))
    }
    fn deserialize_char<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let scalar = self.read_u32()?;
        let c = char::from_u32(scalar).ok_or(Error::InvalidChar(scalar))?;
        visitor.visit_char(c)
    }
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let n = self.read_len()?;
        let bytes = self.take(n)?;
        let s = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
        visitor.visit_borrowed_str(s)
    }
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_str(visitor)
    }
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let n = self.read_len()?;
        let bytes = self.take(n)?;
        visitor.visit_borrowed_bytes(bytes)
    }
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        self.deserialize_bytes(visitor)
    }
    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        match self.read_u8()? {
            0 => visitor.visit_none(),
            1 => visitor.visit_some(self),
            t => Err(Error::InvalidOptionTag(t)),
        }
    }
    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_unit_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_unit()
    }
    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_newtype_struct(self)
    }
    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let len = self.read_len()?;
        visitor.visit_seq(Access {
            de: self,
            remaining: len,
        })
    }
    fn deserialize_tuple<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        visitor.visit_seq(Access {
            de: self,
            remaining: len,
        })
    }
    fn deserialize_tuple_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_seq(Access {
            de: self,
            remaining: len,
        })
    }
    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Error> {
        let len = self.read_len()?;
        visitor.visit_map(Access {
            de: self,
            remaining: len,
        })
    }
    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_seq(Access {
            de: self,
            remaining: fields.len(),
        })
    }
    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        visitor.visit_enum(self)
    }
    fn deserialize_identifier<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        // Structs deserialize field-by-field via `visit_seq`, and enum variants
        // are selected by index in `EnumAccess`, so an identifier is never
        // requested against this non-self-describing format.
        Err(Error::NotSelfDescribing)
    }
    fn deserialize_ignored_any<V: Visitor<'de>>(self, _visitor: V) -> Result<V::Value, Error> {
        Err(Error::NotSelfDescribing)
    }
    fn is_human_readable(&self) -> bool {
        false
    }
}

/// Yields a fixed number of elements (a sequence, tuple, struct fields, or map
/// entries) from the underlying deserializer.
struct Access<'a, 'de> {
    de: &'a mut Deserializer<'de>,
    remaining: usize,
}

impl<'de> de::SeqAccess<'de> for Access<'_, 'de> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

impl<'de> de::MapAccess<'de> for Access<'_, 'de> {
    type Error = Error;
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Error> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(&mut *self.de).map(Some)
    }
    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, Error> {
        seed.deserialize(&mut *self.de)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}

// Enum variants are identified by the `u32` index we wrote; feed it to the seed
// through serde's integer deserializer, then read the variant payload.
impl<'de> de::EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    type Variant = Self;
    fn variant_seed<V: DeserializeSeed<'de>>(self, seed: V) -> Result<(V::Value, Self), Error> {
        let index = self.read_u32()?;
        let value = seed.deserialize(index.into_deserializer())?;
        Ok((value, self))
    }
}

impl<'de> de::VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;
    fn unit_variant(self) -> Result<(), Error> {
        Ok(())
    }
    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, Error> {
        seed.deserialize(self)
    }
    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_tuple(self, len, visitor)
    }
    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error> {
        de::Deserializer::deserialize_tuple(self, fields.len(), visitor)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use super::{Error, from_slice, to_vec};

    fn round<T>(value: &T) -> T
    where
        T: Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let bytes = to_vec(value).expect("serialize");
        let back: T = from_slice(&bytes).expect("deserialize");
        assert_eq!(value, &back, "round trip mismatch");
        back
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    enum Shape {
        Point,
        Radius(f64),
        Rect(u32, u32),
        Named { id: u64, tag: String },
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Everything {
        b: bool,
        i: i64,
        u: u128,
        f: f64,
        c: char,
        s: String,
        bytes: Vec<u8>,
        opt_some: Option<u32>,
        opt_none: Option<u32>,
        list: Vec<Shape>,
        map: BTreeMap<String, i32>,
        nested: Vec<Vec<u8>>,
        unit: (),
        pair: (u8, String),
    }

    #[test]
    fn primitives_round_trip() {
        round(&true);
        round(&false);
        round(&(-12345i64));
        round(&u64::MAX);
        round(&0.0f64);
        round(&(-1.5f32));
        round(&'∆');
        round(&"héllo".to_string());
    }

    #[test]
    fn all_enum_variant_kinds_round_trip() {
        round(&Shape::Point);
        round(&Shape::Radius(2.5));
        round(&Shape::Rect(3, 4));
        round(&Shape::Named {
            id: 7,
            tag: "q".into(),
        });
    }

    #[test]
    fn options_and_collections_round_trip() {
        round(&Some(9u32));
        round(&Option::<u32>::None);
        round(&vec![1u8, 2, 3]);
        let mut m = BTreeMap::new();
        m.insert("a".to_string(), 1i32);
        m.insert("b".to_string(), -2);
        round(&m);
    }

    #[test]
    fn deep_struct_round_trips() {
        let mut map = BTreeMap::new();
        map.insert("x".to_string(), 10);
        map.insert("y".to_string(), -20);
        round(&Everything {
            b: true,
            i: -99,
            u: u128::MAX,
            f: 3.25,
            c: 'z',
            s: "wire".into(),
            bytes: vec![9, 8, 7],
            opt_some: Some(42),
            opt_none: None,
            list: vec![Shape::Point, Shape::Rect(1, 2), Shape::Radius(0.5)],
            map,
            nested: vec![vec![1, 2], vec![], vec![3]],
            unit: (),
            pair: (255, "end".into()),
        });
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = to_vec(&7u32).unwrap();
        bytes.push(0); // one extra byte
        let err = from_slice::<u32>(&bytes).unwrap_err();
        assert!(matches!(err, Error::TrailingBytes(1)), "got {err:?}");
    }

    #[test]
    fn truncated_input_is_eof_not_panic() {
        let bytes = to_vec(&(1u64, 2u64)).unwrap();
        let err = from_slice::<(u64, u64)>(&bytes[..bytes.len() - 1]).unwrap_err();
        assert!(matches!(err, Error::Eof), "got {err:?}");
    }
}
