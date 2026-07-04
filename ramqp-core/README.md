# ramqp-core

The role-agnostic **AMQP 1.0 protocol engine** shared by the
[`ramqp`](https://crates.io/crates/ramqp) client and the in-development
`ramqp-broker`: a clean-room codec and type system for the full OASIS AMQP 1.0
specification, single-pass zero-copy framing, protocol-header and `open`
negotiation, channel multiplexing, heartbeat, and the session/link state
machines (flow-control windows, credit, delivery assembly, settlement).

Most users want a role on top of this engine instead:

- **`ramqp`** — the async AMQP 1.0 client (this crate re-exported).
- **`ramqp-broker`** — a performance-first, highly-available AMQP 1.0 broker
  (in development).

`#![forbid(unsafe_code)]`. MIT license.

## Cargo features

| Feature       | Effect                                                         |
|---------------|----------------------------------------------------------------|
| `scram`       | SCRAM-SHA-1/256/512 primitives (hash math, SASLprep, nonces)   |
| `transaction` | Transaction wire types (clean-room, spec part 4)               |
