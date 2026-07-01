//! AMQP 1.0 SASL security frame bodies (core spec §5.3).

use bytes::{Bytes, BytesMut};

use crate::amqp_composite;
use crate::codec::described::{descriptors, peek_descriptor};
use crate::codec::{Decode, DecodeError, Descriptor, Encode, Symbol};

/// The outcome code of a SASL negotiation (`sasl-code`, a `ubyte`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SaslCode {
    /// Authentication succeeded.
    #[default]
    Ok = 0,
    /// Authentication failed (bad credentials).
    Auth = 1,
    /// A system error occurred (failure not related to credentials).
    Sys = 2,
    /// A permanent system error.
    SysPerm = 3,
    /// A transient system error; retry may succeed.
    SysTemp = 4,
}

impl Encode for SaslCode {
    fn encode(&self, buf: &mut BytesMut) {
        (*self as u8).encode(buf)
    }
}

impl Decode for SaslCode {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            0 => Ok(SaslCode::Ok),
            1 => Ok(SaslCode::Auth),
            2 => Ok(SaslCode::Sys),
            3 => Ok(SaslCode::SysPerm),
            4 => Ok(SaslCode::SysTemp),
            n => Err(DecodeError::InvalidValue(format!("invalid sasl-code {n}"))),
        }
    }
}

amqp_composite! {
    /// `sasl-mechanisms` (`0x40`): the server's advertised mechanism list.
    /// `sasl-server-mechanisms` is mandatory (always present on the wire).
    pub struct SaslMechanisms : descriptors::SASL_MECHANISMS => {
        sasl_server_mechanisms: Vec<Symbol> = req_symbols("sasl-server-mechanisms"),
    }
}

amqp_composite! {
    /// `sasl-init` (`0x41`): the client's selected mechanism + initial response.
    pub struct SaslInit : descriptors::SASL_INIT => {
        mechanism: Symbol = req("mechanism"),
        initial_response: Option<Bytes> = opt(),
        hostname: Option<String> = opt(),
    }
}

amqp_composite! {
    /// `sasl-challenge` (`0x42`): a server challenge.
    pub struct SaslChallenge : descriptors::SASL_CHALLENGE => {
        challenge: Bytes = req("challenge"),
    }
}

amqp_composite! {
    /// `sasl-response` (`0x43`): a client response to a challenge.
    pub struct SaslResponse : descriptors::SASL_RESPONSE => {
        response: Bytes = req("response"),
    }
}

amqp_composite! {
    /// `sasl-outcome` (`0x44`): the negotiation result.
    pub struct SaslOutcome : descriptors::SASL_OUTCOME => {
        code: SaslCode = req("code"),
        additional_data: Option<Bytes> = opt(),
    }
}

/// Any SASL frame body, decoded by descriptor.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs)]
pub enum SaslFrame {
    Mechanisms(SaslMechanisms),
    Init(SaslInit),
    Challenge(SaslChallenge),
    Response(SaslResponse),
    Outcome(SaslOutcome),
}

impl Encode for SaslFrame {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            SaslFrame::Mechanisms(f) => f.encode(buf),
            SaslFrame::Init(f) => f.encode(buf),
            SaslFrame::Challenge(f) => f.encode(buf),
            SaslFrame::Response(f) => f.encode(buf),
            SaslFrame::Outcome(f) => f.encode(buf),
        }
    }
}

impl Decode for SaslFrame {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(match peek_descriptor(buf)? {
            Descriptor::Code(descriptors::SASL_MECHANISMS) => {
                SaslFrame::Mechanisms(SaslMechanisms::decode(buf)?)
            }
            Descriptor::Code(descriptors::SASL_INIT) => SaslFrame::Init(SaslInit::decode(buf)?),
            Descriptor::Code(descriptors::SASL_CHALLENGE) => {
                SaslFrame::Challenge(SaslChallenge::decode(buf)?)
            }
            Descriptor::Code(descriptors::SASL_RESPONSE) => {
                SaslFrame::Response(SaslResponse::decode(buf)?)
            }
            Descriptor::Code(descriptors::SASL_OUTCOME) => {
                SaslFrame::Outcome(SaslOutcome::decode(buf)?)
            }
            other => {
                return Err(DecodeError::InvalidValue(format!(
                    "unknown sasl frame descriptor {other}"
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_slice, to_vec};

    fn rt(f: SaslFrame) {
        let back: SaslFrame = from_slice(&to_vec(&f)).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn sasl_frames_round_trip() {
        rt(SaslFrame::Mechanisms(SaslMechanisms {
            sasl_server_mechanisms: vec![Symbol::new("PLAIN"), Symbol::new("ANONYMOUS")],
        }));
        rt(SaslFrame::Init(SaslInit {
            mechanism: Symbol::new("PLAIN"),
            initial_response: Some(Bytes::from_static(b"\0user\0pass")),
            hostname: Some("broker".into()),
        }));
        rt(SaslFrame::Challenge(SaslChallenge {
            challenge: Bytes::from_static(b"r=abc,s=def,i=4096"),
        }));
        rt(SaslFrame::Response(SaslResponse {
            response: Bytes::from_static(b"c=biws,r=abc"),
        }));
        rt(SaslFrame::Outcome(SaslOutcome {
            code: SaslCode::Ok,
            additional_data: None,
        }));
        rt(SaslFrame::Outcome(SaslOutcome {
            code: SaslCode::Auth,
            additional_data: Some(Bytes::from_static(b"bad")),
        }));
    }

    #[test]
    fn sasl_mechanisms_field_is_mandatory() {
        use crate::codec::DecodeError;
        // even an empty mechanism list encodes the field as a present array, not
        // an elided null.
        let bytes = to_vec(&SaslMechanisms {
            sasl_server_mechanisms: vec![],
        });
        let back: SaslMechanisms = from_slice(&bytes).unwrap();
        assert!(back.sasl_server_mechanisms.is_empty());

        // a sasl-mechanisms described list with zero fields must fail to decode.
        let mut buf = bytes::BytesMut::new();
        crate::codec::encode_described_list(&mut buf, descriptors::SASL_MECHANISMS, |_fw| {});
        let r: Result<SaslMechanisms, _> = from_slice(&buf);
        assert!(matches!(
            r,
            Err(DecodeError::MissingField("sasl-server-mechanisms"))
        ));
    }
}
