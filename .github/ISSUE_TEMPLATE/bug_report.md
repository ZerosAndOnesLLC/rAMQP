---
name: Bug Report
about: Report a bug in ramqp
title: ''
labels: bug
assignees: ''
---

## Description

A clear description of the bug.

## Steps to Reproduce

A minimal code snippet that reproduces the issue:

```rust
// ...
```

1. Connect to...
2. Send / receive...
3. Observe...

## Expected Behavior

What should happen.

## Actual Behavior

What actually happens (include the error / panic message).

## Environment

- **OS**: (e.g. Ubuntu 24.04)
- **ramqp version**: (e.g. 0.5.3)
- **Rust version**: (output of `rustc --version`)
- **Cargo features enabled**: (e.g. `rustls`, `ws`, `scram`, default)
- **Broker + version**: (e.g. RabbitMQ 4.x, ActiveMQ Artemis, Azure Service Bus)
- **Transport**: `amqp` / `amqps` / `ws` / `wss`

## Logs

Relevant logs at debug level (`RUST_LOG=ramqp=debug`, if you have a `tracing` subscriber):

```
paste logs here
```
