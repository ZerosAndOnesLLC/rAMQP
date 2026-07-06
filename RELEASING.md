# Releasing

The repo became a **virtual workspace** in 0.8.0, and that changes how
releases work. This page is the checklist.

## The crates and what publishes

| Crate | crates.io | Publishes? |
|---|---|---|
| `ramqp-core` | new name (verified available) | **Yes — and always first** |
| `ramqp` | exists (0.7.2 published) | Yes, after `ramqp-core` |
| `ramqp-broker` | new name (verified available) | **Not yet** — pre-alpha API; publish once it stabilizes |
| `ramqp-bench-compare` | — | Never (`publish = false`; keeps the `fe2o3-amqp` dev dependency out of the graph) |

## Why order matters (the Cargo.toml mechanics)

`ramqp/Cargo.toml` depends on the engine like this:

```toml
ramqp-core = { path = "../ramqp-core", version = "0.2.0" }
```

Inside the workspace, Cargo uses the `path`. **When publishing, the `path` is
stripped and only `version = "0.2.0"` remains** — so `cargo publish -p ramqp`
fails unless a matching `ramqp-core` already exists on crates.io. Hence:

1. `cargo publish -p ramqp-core`
2. wait for the index (a minute or two)
3. `cargo publish -p ramqp`

`ramqp-broker/Cargo.toml` has the same shaped dependency on `ramqp-core`, so
the same rule applies whenever it starts publishing.

### Keeping the version pins in sync

When bumping `ramqp-core`'s `version` in `ramqp-core/Cargo.toml`, update the
`version = "…"` in **both** dependents (`ramqp/Cargo.toml`,
`ramqp-broker/Cargo.toml`) in the same commit. The workspace builds fine
without doing this (the `path` wins locally) — publishing is what breaks, and
only later. Don't rely on the local build to catch it.

Other manifest facts worth knowing:

- `ramqp`'s `readme = "../README.md"` points above the package directory —
  intentional; Cargo bundles the file into the package, so crates.io renders
  the repo README.
- The `scram`/`transaction` features on `ramqp` delegate to the corresponding
  `ramqp-core` features (`scram = ["ramqp-core/scram", …]`). Feature names
  seen by users are unchanged from 0.7.
- `[profile.bench]` lives in the **root** `Cargo.toml` (profiles are
  workspace-global; Cargo ignores profiles in member manifests).
- Downstream users need **no `Cargo.toml` changes** for 0.8: `ramqp = "0.8"`
  pulls `ramqp-core` transitively. `ramqp-core`/`ramqp-broker` are only for
  people who want the engine alone or an embedded broker.

## Release steps

1. Bump versions (semver: major = breaking, minor = features, patch = fixes)
   and sync the dependents' pins as above. Update `CHANGELOG.md`.
2. `cargo test --all-features && cargo clippy --all-targets --all-features -- -D warnings`
3. Dry-run the packaging (catches stripped-path/readme/include mistakes):
   `cargo publish -p ramqp-core --dry-run`, then
   `cargo publish -p ramqp --dry-run`
   (the `ramqp` dry-run fails against the index until core is actually
   published — expected; it still validates the package contents).
4. Tag `vX.Y.Z` and push the tag. `release.yml` tests, then publishes
   **`ramqp-core` first, then `ramqp`** (in that order, with an index-refresh
   wait between), then creates the GitHub release.
5. Verify: `docs.rs/ramqp` and `docs.rs/ramqp-core` build, and a scratch
   project with `ramqp = "X.Y"` compiles.

## The 0.8.0 release specifically

First release from the workspace; one-time notes:

- Publishes `ramqp-core` **0.2.0** (its first release — 0.1.0 was never
  published) and `ramqp` **0.8.0**.
- `ramqp` 0.8.0 is an internal restructure: the public API is byte-compatible
  with 0.7 (enforced by `tests/public_api.rs`), so downstream upgrades are a
  version-bump only.
- `ramqp-broker` does **not** publish. If someone squats the name in the
  meantime it's recoverable (crates.io policy), but publishing an early
  placeholder `0.0.1` is an option if that risk feels real.
