# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.5.x   | Yes       |

## Reporting a Vulnerability

If you discover a security vulnerability in ramqp, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, email support@zerosandones.us with:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if you have one)

We will acknowledge receipt within 48 hours and provide a timeline for a fix.

## Security Considerations

ramqp is a client library that speaks AMQP 1.0 to message brokers, often over the
network and often with credentials. Key security properties:

### Memory safety
- The crate is `#![forbid(unsafe_code)]` throughout.
- The wire decoder is hardened against malicious/oversized frames: size/count
  hints are clamped to the remaining input before allocation (no
  attacker-controlled pre-allocation), and a configurable `max_message_size`
  bounds reassembled deliveries.
- Internal queues are bounded: the per-link delivery channel is consumer-driven
  (the broker can never push more than the handle can buffer), and
  `Producer::send_settled` is bounded by `LinkConfig.max_outbox` so a slow broker
  cannot grow memory without limit.

### Transport (TLS)
- `amqps://` is available via the `rustls` (default) or `native-tls` feature.
  rustls trusts the Mozilla webpki roots by default; private CAs and client
  certificates (mutual TLS) are supported via `TlsConfig` / the
  `ConnectionBuilder` helpers.
- **`danger_accept_invalid_certs` disables certificate verification and is for
  testing only.** Never enable it against a production broker — it defeats TLS
  authentication and exposes the connection to interception.
- `ws://` is plaintext; use `wss://` (TLS) for WebSocket transport over untrusted
  networks.

### Authentication (SASL)
- `PLAIN` transmits the username and password to the broker — use it only over a
  TLS (`amqps`/`wss`) transport. `ANONYMOUS` and `EXTERNAL` are also supported.
- The `scram` feature adds `SCRAM-SHA-1/256/512`, which does not send the
  password over the wire; the server-signature check is constant-time and the
  iteration count is bounded to reject abusive values.
- Credentials supplied via the connection URL or a `SaslProfile` live in process
  memory and may appear in logs/backtraces if you print them — handle with care.

### Operational
- Validate the broker's certificate (do not disable verification) and pin a
  private CA where appropriate.
- Treat broker-sent error conditions as untrusted input; ramqp surfaces them via
  typed errors rather than acting on them.
