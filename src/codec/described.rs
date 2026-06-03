//! Described-type machinery: descriptors, the descriptor-code registry, and the
//! [`ListDecoder`] that reads composite (described-list) fields with the
//! trailing-`null`/forward-compatibility semantics the spec requires.

use bytes::Bytes;

use super::decode::{
    Decode, DecodeError, peek_code, read_array_header, read_bytes, read_list_header, read_u32,
    read_u8,
};
use super::primitives::{Symbol, codes};

/// The descriptor of a described type: either a numeric `ulong` code (the
/// common case) or a `symbol` name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Descriptor {
    /// `amqp:<name>` numeric descriptor (low 32 bits; the AMQP domain is 0).
    Code(u64),
    /// A symbolic descriptor.
    Symbol(Symbol),
}

impl std::fmt::Display for Descriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Descriptor::Code(c) => write!(f, "{c:#x}"),
            Descriptor::Symbol(s) => write!(f, "{s}"),
        }
    }
}

/// Numeric descriptor codes for every described type this crate handles
/// (low 32 bits; the AMQP domain in the high 32 bits is always zero).
#[allow(missing_docs)]
pub mod descriptors {
    // Transport performatives
    pub const OPEN: u64 = 0x0000_0010;
    pub const BEGIN: u64 = 0x0000_0011;
    pub const ATTACH: u64 = 0x0000_0012;
    pub const FLOW: u64 = 0x0000_0013;
    pub const TRANSFER: u64 = 0x0000_0014;
    pub const DISPOSITION: u64 = 0x0000_0015;
    pub const DETACH: u64 = 0x0000_0016;
    pub const END: u64 = 0x0000_0017;
    pub const CLOSE: u64 = 0x0000_0018;

    // Definitions
    pub const ERROR: u64 = 0x0000_001d;

    // Transactions
    pub const COORDINATOR: u64 = 0x0000_0030;
    pub const DECLARE: u64 = 0x0000_0031;
    pub const DISCHARGE: u64 = 0x0000_0032;
    pub const DECLARED: u64 = 0x0000_0033;
    pub const TRANSACTIONAL_STATE: u64 = 0x0000_0034;

    // Delivery states / outcomes
    pub const RECEIVED: u64 = 0x0000_0023;
    pub const ACCEPTED: u64 = 0x0000_0024;
    pub const REJECTED: u64 = 0x0000_0025;
    pub const RELEASED: u64 = 0x0000_0026;
    pub const MODIFIED: u64 = 0x0000_0027;

    // Addressing
    pub const SOURCE: u64 = 0x0000_0028;
    pub const TARGET: u64 = 0x0000_0029;

    // Lifetime policies
    pub const DELETE_ON_CLOSE: u64 = 0x0000_002b;
    pub const DELETE_ON_NO_LINKS: u64 = 0x0000_002c;
    pub const DELETE_ON_NO_MESSAGES: u64 = 0x0000_002d;
    pub const DELETE_ON_NO_LINKS_OR_MESSAGES: u64 = 0x0000_002e;

    // Message sections
    pub const HEADER: u64 = 0x0000_0070;
    pub const DELIVERY_ANNOTATIONS: u64 = 0x0000_0071;
    pub const MESSAGE_ANNOTATIONS: u64 = 0x0000_0072;
    pub const PROPERTIES: u64 = 0x0000_0073;
    pub const APPLICATION_PROPERTIES: u64 = 0x0000_0074;
    pub const DATA: u64 = 0x0000_0075;
    pub const AMQP_SEQUENCE: u64 = 0x0000_0076;
    pub const AMQP_VALUE: u64 = 0x0000_0077;
    pub const FOOTER: u64 = 0x0000_0078;

    // SASL frames
    pub const SASL_MECHANISMS: u64 = 0x0000_0040;
    pub const SASL_INIT: u64 = 0x0000_0041;
    pub const SASL_CHALLENGE: u64 = 0x0000_0042;
    pub const SASL_RESPONSE: u64 = 0x0000_0043;
    pub const SASL_OUTCOME: u64 = 0x0000_0044;
}

/// Decode a descriptor (`ulong` code or `symbol`) from the cursor.
pub fn decode_descriptor(buf: &mut Bytes) -> Result<Descriptor, DecodeError> {
    match peek_code(buf) {
        Some(codes::ULONG_0 | codes::SMALL_ULONG | codes::ULONG) => {
            Ok(Descriptor::Code(u64::decode(buf)?))
        }
        Some(codes::SYM8 | codes::SYM32) => Ok(Descriptor::Symbol(Symbol::decode(buf)?)),
        Some(c) => Err(DecodeError::InvalidFormatCode {
            code: c,
            expected: "descriptor (ulong or symbol)",
        }),
        None => Err(DecodeError::Eof { needed: 1 }),
    }
}

/// Read the descriptor of a described type **without consuming** the cursor, so
/// a caller can dispatch on it (e.g. choosing which section/outcome to decode).
pub fn peek_descriptor(buf: &Bytes) -> Result<Descriptor, DecodeError> {
    let mut probe = buf.clone();
    match read_u8(&mut probe)? {
        codes::DESCRIBED => decode_descriptor(&mut probe),
        c => Err(DecodeError::InvalidFormatCode {
            code: c,
            expected: "described type (0x00)",
        }),
    }
}

/// Open a described list with the expected numeric descriptor, returning a
/// [`ListDecoder`] positioned at the first field.
pub fn decode_described_list(buf: &mut Bytes, expected: u64) -> Result<ListDecoder, DecodeError> {
    match read_u8(buf)? {
        codes::DESCRIBED => {}
        c => {
            return Err(DecodeError::InvalidFormatCode {
                code: c,
                expected: "described type (0x00)",
            });
        }
    }
    match decode_descriptor(buf)? {
        Descriptor::Code(c) if c == expected => {}
        other => {
            return Err(DecodeError::UnexpectedDescriptor {
                expected,
                found: other.to_string(),
            });
        }
    }
    let (count, body) = read_list_header(buf)?;
    Ok(ListDecoder {
        body,
        remaining: count,
    })
}

/// Read and verify a described-type header (`0x00` + the expected descriptor),
/// leaving the cursor at the body. Used by described map/binary/value sections
/// (annotations, application-properties, data, amqp-value, …).
pub fn expect_descriptor(buf: &mut Bytes, expected: u64) -> Result<(), DecodeError> {
    match read_u8(buf)? {
        codes::DESCRIBED => {}
        c => {
            return Err(DecodeError::InvalidFormatCode {
                code: c,
                expected: "described type (0x00)",
            });
        }
    }
    match decode_descriptor(buf)? {
        Descriptor::Code(c) if c == expected => Ok(()),
        other => Err(DecodeError::UnexpectedDescriptor {
            expected,
            found: other.to_string(),
        }),
    }
}

/// Reads the fields of a described list, applying AMQP elision rules: absent
/// trailing fields and explicit `null`s both decode to the field default.
#[derive(Debug)]
pub struct ListDecoder {
    body: Bytes,
    remaining: u32,
}

impl ListDecoder {
    /// How many encoded fields remain.
    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    fn advance_one(&mut self) {
        self.remaining -= 1;
    }

    /// Decode an optional field: `None` if elided or explicitly `null`.
    pub fn opt<T: Decode>(&mut self) -> Result<Option<T>, DecodeError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.advance_one();
        if peek_code(&self.body) == Some(codes::NULL) {
            let _ = read_u8(&mut self.body)?;
            Ok(None)
        } else {
            Ok(Some(T::decode(&mut self.body)?))
        }
    }

    /// Decode a mandatory field; errors if elided or `null`.
    pub fn req<T: Decode>(&mut self, field: &'static str) -> Result<T, DecodeError> {
        if self.remaining == 0 || peek_code(&self.body) == Some(codes::NULL) {
            if self.remaining > 0 {
                self.advance_one();
            }
            return Err(DecodeError::MissingField(field));
        }
        self.advance_one();
        T::decode(&mut self.body)
    }

    /// Decode a mandatory `multiple` symbol field; errors if absent or `null`.
    pub fn req_symbols(&mut self, field: &'static str) -> Result<Vec<Symbol>, DecodeError> {
        if self.remaining == 0 || peek_code(&self.body) == Some(codes::NULL) {
            if self.remaining > 0 {
                self.advance_one();
            }
            return Err(DecodeError::MissingField(field));
        }
        self.advance_one();
        match peek_code(&self.body) {
            Some(codes::ARRAY8) | Some(codes::ARRAY32) => {
                let (count, body) = read_array_header(&mut self.body)?;
                decode_symbol_array(body, count)
            }
            _ => Ok(vec![Symbol::decode(&mut self.body)?]),
        }
    }

    /// Decode a `multiple` symbol field: accepts an array, a single symbol, an
    /// explicit `null`, or an elided field (all but the array yield 0–1 items).
    pub fn symbols(&mut self) -> Result<Vec<Symbol>, DecodeError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.advance_one();
        match peek_code(&self.body) {
            Some(codes::NULL) => {
                let _ = read_u8(&mut self.body)?;
                Ok(Vec::new())
            }
            Some(codes::ARRAY8) | Some(codes::ARRAY32) => {
                let (count, body) = read_array_header(&mut self.body)?;
                decode_symbol_array(body, count)
            }
            _ => Ok(vec![Symbol::decode(&mut self.body)?]),
        }
    }
}

/// Define an AMQP composite (described-list) type with its [`Encode`] and
/// [`Decode`] implementations from a concise field list.
///
/// Each field is declared as `name: Type = kind(args)`, where `kind` is one of:
/// - `req("wire-name")` — mandatory; errors if absent/`null`.
/// - `opt()` — `Option<T>`; `None` when absent/`null`.
/// - `default(value)` — `T`; uses `value` when absent/`null`.
/// - `symbols()` — `Vec<Symbol>` `multiple` field (array, single, or absent).
///
/// Fields encode in declaration order; trailing `null`s are elided by the
/// underlying [`encode_described_list`](crate::codec::encode_described_list).
///
/// [`Encode`]: crate::codec::Encode
/// [`Decode`]: crate::codec::Decode
#[macro_export]
macro_rules! amqp_composite {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident : $desc:expr => {
            $(
                $(#[$fmeta:meta])*
                $fname:ident : $fty:ty = $kind:ident ( $($karg:tt)* )
            ),* $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq)]
        #[allow(missing_docs)]
        $vis struct $name {
            $( $(#[$fmeta])* pub $fname : $fty , )*
        }

        // `Default` honors each field's declared default — in particular a
        // `default(expr)` field defaults to `expr` (the spec value), not the
        // type's own `Default` (e.g. `expiry_policy` defaults to `session-end`,
        // never an empty symbol, which strict brokers reject).
        impl ::core::default::Default for $name {
            fn default() -> Self {
                Self {
                    $( $fname : $crate::amqp_composite!(@default $kind ( $($karg)* )) , )*
                }
            }
        }

        impl $crate::codec::Encode for $name {
            fn encode(&self, buf: &mut ::bytes::BytesMut) {
                $crate::codec::encode_described_list(buf, $desc, |fw| {
                    let _ = &fw;
                    $( $crate::amqp_composite!(@enc fw, self, $fname, $kind ( $($karg)* )); )*
                });
            }
        }

        impl $crate::codec::Decode for $name {
            fn decode(buf: &mut ::bytes::Bytes)
                -> ::core::result::Result<Self, $crate::codec::DecodeError>
            {
                #[allow(unused_mut, unused_variables)]
                let mut d = $crate::codec::decode_described_list(buf, $desc)?;
                Ok(Self {
                    $( $fname : $crate::amqp_composite!(@dec d, $kind ( $($karg)* )) , )*
                })
            }
        }
    };

    // ---- encode arms (encode in field order) ----
    (@enc $fw:ident, $self:ident, $fname:ident, req ( $($n:tt)* )) => { $fw.field(&$self.$fname); };
    (@enc $fw:ident, $self:ident, $fname:ident, opt ( )) => { $fw.field(&$self.$fname); };
    (@enc $fw:ident, $self:ident, $fname:ident, default ( $def:expr )) => { $fw.field(&$self.$fname); };
    (@enc $fw:ident, $self:ident, $fname:ident, symbols ( )) => { $fw.symbols(&$self.$fname); };
    (@enc $fw:ident, $self:ident, $fname:ident, req_symbols ( $($n:tt)* )) => { $fw.symbols_required(&$self.$fname); };

    // ---- default arms (field-aware `Default::default`) ----
    (@default req ( $($n:tt)* )) => { ::core::default::Default::default() };
    (@default opt ( )) => { ::core::option::Option::None };
    (@default default ( $def:expr )) => { $def };
    (@default symbols ( )) => { ::std::vec::Vec::new() };
    (@default req_symbols ( $($n:tt)* )) => { ::std::vec::Vec::new() };

    // ---- decode arms ----
    (@dec $d:ident, req ( $n:expr )) => { $d.req($n)? };
    (@dec $d:ident, opt ( )) => { $d.opt()? };
    (@dec $d:ident, default ( $def:expr )) => { $d.opt()?.unwrap_or($def) };
    (@dec $d:ident, symbols ( )) => { $d.symbols()? };
    (@dec $d:ident, req_symbols ( $n:expr )) => { $d.req_symbols($n)? };
}

/// Decode the body of a symbol array (shared constructor + bare element bodies).
fn decode_symbol_array(mut body: Bytes, count: u32) -> Result<Vec<Symbol>, DecodeError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let ctor = read_u8(&mut body)?;
    // Clamp the capacity hint to the input (each symbol element needs ≥ 1 byte).
    let mut out = Vec::with_capacity((count as usize).min(body.len()));
    for _ in 0..count {
        let len = match ctor {
            codes::SYM8 => read_u8(&mut body)? as usize,
            codes::SYM32 => read_u32(&mut body)? as usize,
            c => {
                return Err(DecodeError::InvalidFormatCode {
                    code: c,
                    expected: "symbol array element",
                });
            }
        };
        let raw = read_bytes(&mut body, len)?;
        let s = String::from_utf8(raw.to_vec())
            .map_err(|_| DecodeError::InvalidUtf8 { kind: "symbol" })?;
        out.push(Symbol(s));
    }
    Ok(out)
}
