#![no_main]
//! Fuzz the AMQP type-system value decoder. Every performative, message
//! section, and delivery annotation the broker parses bottoms out in this
//! decoder, so an arbitrary-bytes-to-`Value` decode must never panic or hang —
//! only decode or error.

use libfuzzer_sys::fuzz_target;
use ramqp_core::codec::{Value, from_slice};

fuzz_target!(|data: &[u8]| {
    let _ = from_slice::<Value>(data);
});
