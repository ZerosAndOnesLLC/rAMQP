# ramqp-core

The role-agnostic **AMQP 1.0 protocol engine** shared by the
[`ramqp`](https://crates.io/crates/ramqp) client and the in-development
`ramqp-broker`: a clean-room implementation of the OASIS AMQP 1.0
specification with no external AMQP dependencies.

What lives here:

- **Codec + type system** — the complete wire codec and every AMQP 1.0 type
  (performatives, messaging sections, SASL frames, transaction types).
- **Transport** — single-pass zero-copy framing (`FramedTransport`), protocol
  headers in both orders (client `negotiate` and server `accept`), address
  parsing, the `IoStream` abstraction.
- **Connection building blocks** — `open` negotiation/reconciliation, channel
  multiplexing, idle-timeout heartbeat.
- **Session + link state machines, both polarities** — flow-control windows,
  credit, delivery assembly, settlement, and the server-side
  `accept_peer_begin`/`accept_peer_attach` alongside the client-side attach
  paths.
- **SASL** — SCRAM-SHA-1/256/512 primitives plus the RFC 5802 server state
  machine (`ScramServer`, verifier-based credential storage) and PLAIN
  parsing; the client state machine lives in `ramqp`.
- **Contracts** — the flat classified error model, id newtypes, config,
  metrics/event observability hooks.

Most users want a role on top of this engine instead:

- **`ramqp`** — the async AMQP 1.0 client (re-exports this crate, so
  `ramqp::...` paths are the stable way in).
- **`ramqp-broker`** — a performance-first, highly-available AMQP 1.0 broker
  (in development).

`#![forbid(unsafe_code)]`. MIT license.

## Cargo features

| Feature       | Effect                                                          |
|---------------|-----------------------------------------------------------------|
| `scram`       | SCRAM-SHA-1/256/512 primitives + the RFC 5802 server machinery  |
| `transaction` | Transaction wire types (clean-room, spec part 4)                |
