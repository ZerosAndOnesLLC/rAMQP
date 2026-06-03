//! AMQP protocol-header negotiation (the 8-byte `AMQP` preamble exchanged
//! before any frames).

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::{ConnectError, ErrorKind};

use super::IoStream;

/// The 8-byte protocol header: `"AMQP"` + protocol-id + version triple.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolHeader {
    /// `0` = AMQP, `2` = TLS, `3` = SASL.
    pub protocol_id: u8,
    /// Major version (1).
    pub major: u8,
    /// Minor version (0).
    pub minor: u8,
    /// Revision (0).
    pub revision: u8,
}

impl ProtocolHeader {
    /// The AMQP 1.0.0 header (`AMQP\x00\x01\x00\x00`).
    pub const AMQP: ProtocolHeader = ProtocolHeader {
        protocol_id: 0,
        major: 1,
        minor: 0,
        revision: 0,
    };
    /// The TLS header (`AMQP\x02\x01\x00\x00`).
    pub const TLS: ProtocolHeader = ProtocolHeader {
        protocol_id: 2,
        major: 1,
        minor: 0,
        revision: 0,
    };
    /// The SASL header (`AMQP\x03\x01\x00\x00`).
    pub const SASL: ProtocolHeader = ProtocolHeader {
        protocol_id: 3,
        major: 1,
        minor: 0,
        revision: 0,
    };

    /// The 8 wire bytes for this header.
    pub fn to_bytes(self) -> [u8; 8] {
        [
            b'A',
            b'M',
            b'Q',
            b'P',
            self.protocol_id,
            self.major,
            self.minor,
            self.revision,
        ]
    }

    /// Parse an 8-byte header, validating the `"AMQP"` magic.
    pub fn from_bytes(bytes: [u8; 8]) -> Result<Self, ConnectError> {
        if &bytes[0..4] != b"AMQP" {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!("bad protocol header magic: {:02x?}", &bytes[0..4]),
            ));
        }
        Ok(ProtocolHeader {
            protocol_id: bytes[4],
            major: bytes[5],
            minor: bytes[6],
            revision: bytes[7],
        })
    }

    /// Write this header to `stream`.
    pub async fn write<S: IoStream>(self, stream: &mut S) -> Result<(), ConnectError> {
        stream.write_all(&self.to_bytes()).await?;
        stream.flush().await?;
        Ok(())
    }

    /// Read an 8-byte header from `stream`.
    pub async fn read<S: IoStream>(stream: &mut S) -> Result<Self, ConnectError> {
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).await?;
        ProtocolHeader::from_bytes(buf)
    }

    /// Send our header and read the peer's, erroring on a version mismatch.
    ///
    /// Per the spec a peer that disagrees replies with the header it *does*
    /// support before closing; we surface that as a protocol error including the
    /// peer's offered version.
    pub async fn negotiate<S: IoStream>(self, stream: &mut S) -> Result<(), ConnectError> {
        self.write(stream).await?;
        let peer = ProtocolHeader::read(stream).await?;
        if peer != self {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!(
                    "protocol header mismatch: sent {:?}, peer offered {:?}",
                    self.to_bytes(),
                    peer.to_bytes()
                ),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amqp_header_bytes() {
        assert_eq!(ProtocolHeader::AMQP.to_bytes(), *b"AMQP\x00\x01\x00\x00");
        assert_eq!(ProtocolHeader::SASL.to_bytes(), *b"AMQP\x03\x01\x00\x00");
        assert_eq!(ProtocolHeader::TLS.to_bytes(), *b"AMQP\x02\x01\x00\x00");
    }

    #[test]
    fn roundtrip_and_magic_check() {
        let h = ProtocolHeader::AMQP;
        assert_eq!(ProtocolHeader::from_bytes(h.to_bytes()).unwrap(), h);
        assert!(ProtocolHeader::from_bytes(*b"XXXX\x00\x01\x00\x00").is_err());
    }

    #[tokio::test]
    async fn negotiate_over_duplex_matches() {
        let (mut a, mut b) = tokio::io::duplex(64);
        let server = tokio::spawn(async move {
            // echo the same header back
            let h = ProtocolHeader::read(&mut b).await.unwrap();
            h.write(&mut b).await.unwrap();
        });
        ProtocolHeader::AMQP.negotiate(&mut a).await.unwrap();
        server.await.unwrap();
    }
}
