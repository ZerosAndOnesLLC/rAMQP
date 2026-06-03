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
| `ws`          | `ws://` / `wss://` (AMQP over WebSocket)                     |
| `scram`       | SASL `SCRAM-SHA-1/256/512`                                   |
| `transaction` | Transaction coordinator types (clean-room, spec part 4)     |

ANONYMOUS / PLAIN / EXTERNAL SASL are always available.

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
- Reconnect backoff + resilient connect + a health-aware connection pool.
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

**Verified against RabbitMQ 4.3.1**: SASL PLAIN, open/begin/attach over
`/queues/…` addressing, credit flow, transfer, settlement, and a 100-message
bulk round-trip all interoperate (the broker identifies the connection as
AMQP 1.0 and the queue drains cleanly).

In progress: transparent *mid-stream* reconnect with unsettled replay. The
building blocks are in place — jittered backoff, resilient connect, connection
pool, snapshot-able settlement state, and the
[resume decision matrix](src/link/resume.rs) (resend/resume/settle/abort) — the
remaining work is the supervisor that drives re-attach + replay end to end.

## License

MIT.
