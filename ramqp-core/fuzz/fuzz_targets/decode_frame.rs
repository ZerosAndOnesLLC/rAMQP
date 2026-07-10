#![no_main]
//! Fuzz the frame decoder — the very first thing the broker runs on every byte
//! an untrusted client sends. It must never panic, hang, or over-allocate on
//! arbitrary input: it either decodes a frame, asks for more bytes, or returns
//! a protocol error. We drain repeatedly so multi-frame and trailing-garbage
//! inputs are exercised too.

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use ramqp_core::transport::frame::decode_frame;

fuzz_target!(|data: &[u8]| {
    let mut buf = BytesMut::from(data);
    // A 1 MiB cap mirrors a generous negotiated max-frame-size. Loop until the
    // decoder stops making progress (needs-more, error, or empty).
    loop {
        match decode_frame(&mut buf, 1 << 20) {
            Ok(Some(_frame)) => {
                if buf.is_empty() {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
});
