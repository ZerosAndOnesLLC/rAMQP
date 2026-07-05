//! AMQP 1.0 primitive type-system building blocks: format-code constants and
//! the small wrapper types ([`Symbol`], [`OrderedMap`]) that the typed and
//! dynamic codecs share.
//!
//! These are pure data definitions; the [`Encode`](super::encode::Encode) and
//! [`Decode`](super::decode::Decode) implementations live in the sibling
//! `encode`/`decode` modules.

use std::borrow::Borrow;
use std::ops::Deref;

/// AMQP 1.0 primitive format codes (§1.6 of the core spec).
///
/// The high nibble of a fixed/variable code encodes its category/width; the
/// constants below name every code this crate emits or accepts.
#[allow(missing_docs)]
pub mod codes {
    // Special / zero-width
    pub const DESCRIBED: u8 = 0x00;
    pub const NULL: u8 = 0x40;
    pub const BOOL_TRUE: u8 = 0x41;
    pub const BOOL_FALSE: u8 = 0x42;
    pub const UINT_0: u8 = 0x43;
    pub const ULONG_0: u8 = 0x44;
    pub const LIST_0: u8 = 0x45;

    // Fixed one-byte
    pub const BOOL: u8 = 0x56;
    pub const UBYTE: u8 = 0x50;
    pub const BYTE: u8 = 0x51;
    pub const SMALL_UINT: u8 = 0x52;
    pub const SMALL_ULONG: u8 = 0x53;
    pub const SMALL_INT: u8 = 0x54;
    pub const SMALL_LONG: u8 = 0x55;

    // Fixed two-byte
    pub const USHORT: u8 = 0x60;
    pub const SHORT: u8 = 0x61;

    // Fixed four-byte
    pub const UINT: u8 = 0x70;
    pub const INT: u8 = 0x71;
    pub const FLOAT: u8 = 0x72;
    pub const CHAR: u8 = 0x73;
    pub const DECIMAL32: u8 = 0x74;

    // Fixed eight-byte
    pub const ULONG: u8 = 0x80;
    pub const LONG: u8 = 0x81;
    pub const DOUBLE: u8 = 0x82;
    pub const TIMESTAMP: u8 = 0x83;
    pub const DECIMAL64: u8 = 0x84;

    // Fixed sixteen-byte
    pub const DECIMAL128: u8 = 0x94;
    pub const UUID: u8 = 0x98;

    // Variable one-byte length
    pub const VBIN8: u8 = 0xa0;
    pub const STR8: u8 = 0xa1;
    pub const SYM8: u8 = 0xa3;

    // Variable four-byte length
    pub const VBIN32: u8 = 0xb0;
    pub const STR32: u8 = 0xb1;
    pub const SYM32: u8 = 0xb3;

    // Compound one-byte size+count
    pub const LIST8: u8 = 0xc0;
    pub const MAP8: u8 = 0xc1;

    // Compound four-byte size+count
    pub const LIST32: u8 = 0xd0;
    pub const MAP32: u8 = 0xd1;

    // Array one/four-byte size+count
    pub const ARRAY8: u8 = 0xe0;
    pub const ARRAY32: u8 = 0xf0;
}

/// An AMQP `symbol`: ASCII string values from a constrained domain (capability
/// names, condition names, etc.). A plain owned [`String`] backs it — symbols
/// are short and few.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Symbol(pub String);

impl Symbol {
    /// Construct a symbol from anything string-like.
    pub fn new(s: impl Into<String>) -> Self {
        Symbol(s.into())
    }

    /// Borrow the underlying ASCII text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Symbol {
    fn from(s: &str) -> Self {
        Symbol(s.to_owned())
    }
}

impl From<String> for Symbol {
    fn from(s: String) -> Self {
        Symbol(s)
    }
}

impl Deref for Symbol {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for Symbol {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for Symbol {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// An insertion-ordered map with `PartialEq`-based key lookup.
///
/// AMQP `map`s preserve order and forbid duplicate keys, but the key space is
/// arbitrary AMQP values, so a hash map is awkward. This thin `Vec`-backed type
/// keeps order, supports arbitrary key types, and avoids pulling a heavier
/// dependency into the wire layer.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OrderedMap<K, V>(Vec<(K, V)>);

impl<K, V> OrderedMap<K, V> {
    /// An empty map.
    pub fn new() -> Self {
        OrderedMap(Vec::new())
    }

    /// An empty map with room for `n` entries.
    pub fn with_capacity(n: usize) -> Self {
        OrderedMap(Vec::with_capacity(n))
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map has no entries.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterate entries in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    /// Append an entry without checking for duplicates (decode fast path).
    pub fn push(&mut self, k: K, v: V) {
        self.0.push((k, v));
    }

    /// The backing pairs.
    pub fn as_slice(&self) -> &[(K, V)] {
        &self.0
    }
}

impl<K: PartialEq, V> OrderedMap<K, V> {
    /// Insert or replace, returning the previous value if the key existed.
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        if let Some(slot) = self.0.iter_mut().find(|(ek, _)| *ek == k) {
            Some(std::mem::replace(&mut slot.1, v))
        } else {
            self.0.push((k, v));
            None
        }
    }

    /// Look up by borrowed key.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: PartialEq + ?Sized,
    {
        self.0
            .iter()
            .find(|(k, _)| k.borrow() == key)
            .map(|(_, v)| v)
    }
}

impl<K, V> From<Vec<(K, V)>> for OrderedMap<K, V> {
    fn from(v: Vec<(K, V)>) -> Self {
        OrderedMap(v)
    }
}

impl<K, V> FromIterator<(K, V)> for OrderedMap<K, V> {
    fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        OrderedMap(iter.into_iter().collect())
    }
}

impl<K, V> IntoIterator for OrderedMap<K, V> {
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<(K, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
