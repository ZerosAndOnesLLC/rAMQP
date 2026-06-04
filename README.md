# ramqp

A from-scratch, **clean-room** AMQP 1.0 **client** for Rust, built on `tokio`.

`ramqp` implements the OASIS AMQP 1.0 specification from the ground up — including
its own type/encoding layer — with no external AMQP dependencies. It is designed
to fix the resilience, performance, API, and observability gaps common to existing
clients.

## Highlights

- **Single-pass, zero-copy framing** — bodies are exposed as `bytes::Bytes`
  slices; the transfer/body split is computed once from the negotiated
  `max-frame-size`, never by trial re-serialization.
- **Lock-free actor runtime** — one owning driver task per connection holds all
  protocol state; user handles are cheap clones that exchange messages over
  bounded channels. No locks on the per-message path.
- **Lazy receive by default** — `recv()` yields a `Delivery` exposing raw bytes;
  typed decoding (`.message()`, `.decode::<T>()`) is an explicit, cheap opt-in.
- **Flat, classified errors** — one error type per operation (`ConnectError`,
  `SendError`, `RecvError`, `SessionError`, `LinkError`) with `source()` chains,
  `is_retryable()`/`is_fatal()`, and typed access to any peer-sent error.
- **Pluggable observability** — a `Metrics` trait and a connection-event stream,
  usable without enabling `tracing`.
- **Transparent reconnect** — opt in with `ConnectionBuilder::reconnecting(true)`
  and your `Producer`/`Consumer` handles survive a broker drop: the connection is
  re-established with backoff, sessions/links are re-attached, and in-flight sends
  are replayed — all behind the scenes.
- **`#![forbid(unsafe_code)]`** throughout.

## Quick start

```rust,no_run
use ramqp::{Connection, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open("amqp://guest:guest@localhost:5672").await?;
    let session = conn.begin_session().await?;

    // Produce
    let producer = session.create_producer("queue://demo").await?;
    let outcome = producer.send(Message::text("hello")).await?;
    println!("outcome: {outcome:?}");

    // Consume
    let mut consumer = session.create_consumer("queue://demo").await?;
    let delivery = consumer.recv().await?;
    println!("got: {:?}", delivery.message()?);
    consumer.accept(&delivery).await?;

    conn.close().await?;
    Ok(())
}
```

See [`examples/produce_consume.rs`](examples/produce_consume.rs).

## Cargo features

| Feature       | Effect                                                        |
|---------------|--------------------------------------------------------------|
| `rustls`      | `amqps://` via rustls + webpki-roots                         |
| `native-tls`  | `amqps://` via native-tls                                   |
| `ws`          | `ws://` (AMQP over WebSocket); `wss://` also needs `rustls`  |
| `scram`       | SASL `SCRAM-SHA-1/256/512`                                   |
| `transaction` | Transaction coordinator types (clean-room, spec part 4)     |

ANONYMOUS / PLAIN / EXTERNAL SASL are always available. `wss://` (WebSocket over
TLS) requires **both** `ws` and `rustls`; the `ws` feature alone covers only
plaintext `ws://`.

## Architecture

```
PUBLIC API     Connection · Session · Producer · Consumer        (src/api)
RESILIENCE     supervisor · reconnect · replay · pool            (src/resilience)
LINK           sender/receiver · settlement · credit · delivery  (src/link)
SESSION        begin/end · windows · handle registry             (src/session)
CONNECTION     driver task · open/close · mux · heartbeat        (src/connection)
TRANSPORT      TCP/TLS/WS · single-pass frame codec · SASL       (src/transport, src/sasl)
CONTRACTS      errors · ids · config · metrics/events · proto    (src/{error,ids,config,observe,proto})
CODEC + TYPES  clean-room AMQP 1.0 type system + wire codec      (src/codec, src/types)
```

## Status

Working today (with tests):

- Clean-room codec + full AMQP 1.0 type system (spec-audited).
- TCP / TLS / WebSocket transports; SASL ANONYMOUS / PLAIN / EXTERNAL / SCRAM.
- Connection open/close, heartbeat, channel mux.
- Session begin/end with flow-control windows.
- Link attach/detach, credit/window-gated send with multi-frame split,
  delivery assembly, first/second-stage settlement; producer/consumer handles
  with graceful drop.
- Resilience: jittered reconnect backoff, resilient connect, a health-aware
  connection pool, a bounded fire-and-forget outbox, and **transparent
  mid-stream reconnect** (re-attach + unsettled replay) via
  `ConnectionBuilder::reconnecting(true)`.
- Custom-CA / mutual-TLS / SNI-override `amqps` (`rustls` or `native-tls`).
- Feature-gated transaction coordinator.
- Pluggable metrics + connection-event subscription.

### Tests & benchmarks

```sh
cargo test                                  # unit + mock-peer integration
cargo bench --bench codec                   # codec/framing micro-benchmarks

# Real-broker interop (skipped unless RAMQP_BROKER_URL is set). Example against
# RabbitMQ 4.x (which speaks AMQP 1.0 natively); declare the queue first:
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/my-queue \
    cargo test --test broker -- --test-threads=1
```

**Verified against live RabbitMQ 4.x and ActiveMQ Artemis**: SASL PLAIN,
open/begin/attach, credit flow, transfer, settlement, the accept/reject/release/
modify outcomes, and 100-message bulk round-trips — plus `amqps` (custom-CA TLS),
AMQP-over-WebSocket, and a transparent reconnect across a mid-stream drop. A 45-second
soak sustained ~170k messages with flat memory.

Roadmap: wire-level link resumption (`transfer.resume` + unsettled-map exchange)
to upgrade the current re-attach + at-least-once resend to in-place resume, and
interop coverage for more brokers (Azure Service Bus, Qpid). The
[resume decision matrix](src/link/resume.rs) (resend/resume/settle/abort) is
already in place for it.

## License

MIT.
