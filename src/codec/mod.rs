//! Clean-room AMQP 1.0 type system and wire codec (Phase E).
//!
//! Hand-rolled, single-pass and zero-copy: [`Encode`] writes into a
//! [`bytes::BytesMut`]; [`Decode`] reads from a cursor over [`bytes::Bytes`]
//! so primitive byte runs (binary, string, symbol) are borrowed slices, not
//! copies.

pub mod decode;
pub mod described;
pub mod encode;
pub mod primitives;
pub mod value;

pub use decode::{Decode, DecodeError, peek_code};
pub use described::{
    Descriptor, ListDecoder, decode_described_list, decode_descriptor, descriptors,
    peek_descriptor,
};
pub use encode::{
    Encode, FieldWriter, encode_described_list, encode_descriptor, encode_map_entries, encode_null,
    encode_symbol_array,
};
pub use primitives::{OrderedMap, Symbol, codes};
pub use value::Value;

use bytes::{Bytes, BytesMut};

/// Encode a value into a fresh [`BytesMut`].
pub fn to_bytes<T: Encode + ?Sized>(value: &T) -> BytesMut {
    let mut buf = BytesMut::new();
    value.encode(&mut buf);
    buf
}

/// Encode a value into a `Vec<u8>`.
pub fn to_vec<T: Encode + ?Sized>(value: &T) -> Vec<u8> {
    to_bytes(value).to_vec()
}

/// Decode a value from an owned [`Bytes`] cursor.
pub fn from_bytes<T: Decode>(mut buf: Bytes) -> Result<T, DecodeError> {
    T::decode(&mut buf)
}

/// Decode a value from a borrowed byte slice (copies into an owned buffer).
pub fn from_slice<T: Decode>(slice: &[u8]) -> Result<T, DecodeError> {
    T::decode(&mut Bytes::copy_from_slice(slice))
}
