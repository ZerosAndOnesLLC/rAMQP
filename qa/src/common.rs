//! Helpers shared by the subcommands.

use std::time::Duration;

use ramqp::codec::Value;
use ramqp::types::messaging::Body;
use ramqp::{Connection, ConnectionBuilder, Delivery};

/// A text body: `tag`, a `|`, then padding up to `size` bytes.
pub fn body(tag: &str, size: usize) -> String {
    let mut s = String::with_capacity(size.max(tag.len() + 1));
    s.push_str(tag);
    s.push('|');
    while s.len() < size {
        s.push('x');
    }
    s
}

/// The tag of a body built by [`body`].
pub fn tag(delivery: &Delivery) -> Option<String> {
    match delivery.message().ok()?.body {
        Body::Value(Value::String(s)) => s.split_once('|').map(|(t, _)| t.to_owned()),
        _ => None,
    }
}

/// Connect, with an overall time limit.
pub async fn connect(url: &str, reconnecting: bool) -> Result<Connection, String> {
    let fut = ConnectionBuilder::new(url)
        .reconnecting(reconnecting)
        .connect();
    match tokio::time::timeout(Duration::from_secs(10), fut).await {
        Ok(Ok(c)) => Ok(c),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err("connect timed out".into()),
    }
}

/// A small, fast PRNG (xorshift64*): randomness for churn, not security.
pub struct Rng(u64);

impl Rng {
    pub fn seeded(salt: u64) -> Self {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        Rng((t ^ salt.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..n` (`n > 0`).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Write `value` as pretty JSON to `path`, if given.
pub fn write_json(path: Option<&std::path::Path>, value: &serde_json::Value) {
    if let Some(path) = path
        && let Err(e) = std::fs::write(
            path,
            serde_json::to_string_pretty(value).unwrap_or_default(),
        )
    {
        eprintln!("could not write {}: {e}", path.display());
    }
}
