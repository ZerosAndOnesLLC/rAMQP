# Releasing

The repo became a **virtual workspace** in 0.8.0, and that changes how
releases work. This page is the checklist.

## The crates and what publishes

| Crate | crates.io | Publishes? |
|---|---|---|
| `ramqp-core` | new name (verified available) | **Yes — and always first** |
| `ramqp` | exists (0.7.2 published) | Yes, after `ramqp-core` |
| `ramqp-broker` | first publish: **0.9.0** | Yes — after `ramqp-core` (its only registry dependency; the `ramqp` dev-dependency is path-only and stripped at packaging) |
| `ramqp-bench-compare` | — | Never (`publish = false`; keeps the `fe2o3-amqp` dev dependency out of the graph) |

## Why order matters (the Cargo.toml mechanics)

`ramqp/Cargo.toml` depends on the engine like this:

```toml
ramqp-core = { path = "../ramqp-core", version = "0.2.1" }
```

Inside the workspace, Cargo uses the `path`. **When publishing, the `path` is
stripped and only `version = "0.2.1"` remains** — so `cargo publish -p ramqp`
fails unless a matching `ramqp-core` already exists on crates.io. Hence:

1. `cargo publish -p ramqp-core`
2. wait for the index (a minute or two)
3. `cargo publish -p ramqp`

`ramqp-broker/Cargo.toml` has the same shaped dependency on `ramqp-core`, so
the same rule applies to it: core first, then the broker (`release.yml` does
core → ramqp → broker). The broker's first-ever publish needs the token to
have the **publish-new** scope, and it resets the version to a deliberate
**0.9.0** (the pre-publish 0.8.x patch numbers were per-commit workspace
churn, not a release cadence).

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

## Prerequisites (one-time)

- **`CARGO_REGISTRY_TOKEN` repository secret** — `release.yml` publishes with
  it, and the job fails without it. Create a token at
  <https://crates.io/settings/tokens> with the **publish-update** scope (add
  **publish-new** for a crate's first-ever publish), then add it under
  **Settings → Secrets and variables → Actions → New repository secret**, named
  exactly `CARGO_REGISTRY_TOKEN`.
- **crates.io account** must own (or be able to claim) the crate names — the
  `ramqp-core` name is claimed on its first `cargo publish`.
- **Toolchain**: the workspace tracks **latest stable Rust** (there is no pinned
  MSRV — the broker's `openraft` tree needs ≥ 1.88). Run `rustup update stable`
  before releasing.

## Release steps



1. Bump versions (semver: major = breaking, minor = features, patch = fixes)
   and sync the dependents' pins as above. Update `CHANGELOG.md`: change the
   `## [X.Y.Z] - unreleased` heading to `## [X.Y.Z] - YYYY-MM-DD` (today's date)
   and confirm the entries are accurate.
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

First release from the workspace.

**Current state (on `main`):** all code + CI is merged and green. Versions are
`ramqp-core` **0.2.1**, `ramqp` **0.8.0**, `ramqp-broker` **0.1.10** (the
broker's patch number is just per-commit workspace churn, not a release
cadence — reset it to a deliberate value if/when it first publishes).
`cargo publish -p ramqp-core --dry-run` passes; the pins are synced to 0.2.1.

**What's left to actually ship 0.8.0** (do these, in order):

1. **Decide `ramqp-broker`'s publish policy** — it does **not** publish by
   default (pre-alpha; not yet durable — durability is broker.md Phase 7):
   - **A — keep it back (recommended):** add `publish = false` to
     `ramqp-broker/Cargo.toml` so it can't be published by accident. Ship only
     `ramqp-core` + `ramqp` now; publish the broker once Phase 7 lands and its
     config/auth/addressing surface settles.
   - **B — reserve the name:** publish a placeholder `ramqp-broker 0.0.1`.
   - **C — publish it for real:** only after Phase 7 durability; you'd also want
     `#[non_exhaustive]` on `BrokerConfig` and a "pre-alpha" README banner first.
2. **Stamp the CHANGELOG** `0.8.0` heading with today's date (Release step 1).
3. **Confirm the `CARGO_REGISTRY_TOKEN` secret** exists (Prerequisites).
4. **Dry-run** `cargo publish -p ramqp-core --dry-run` (Release step 3).
5. **Tag `v0.8.0` and push the tag** → `release.yml` publishes `ramqp-core`
   then `ramqp`, then creates the GitHub release.

Notes:

- `ramqp` 0.8.0 is an internal restructure: the public API is byte-compatible
  with 0.7 (enforced by `tests/public_api.rs`), so downstream upgrades are a
  version-bump only.
- `ramqp-broker`: if someone squats the name while it's held back it's
  recoverable (crates.io policy), so option A is safe.
