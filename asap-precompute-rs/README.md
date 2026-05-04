# asap-precompute-rs

`asap-precompute-rs` is the Rust mirror of
[`asap-precompute-go`](../asap-precompute-go/), the host-neutral
edge precompute runtime described in
[`docs/design-asap-edge-framework.md`](../docs/design-asap-edge-framework.md)
§6 and pinned by
[ADR-0002](../docs/adr/adr-0002-extract-precompute-runtime.md).

This crate owns the windowing, snapshot caching, and delta encoding
runtime logic that today lives inside `ASAPQuery-backend`'s ingest
path (`asap-query-engine/src/precompute_operators/*.rs` and
`drivers/ingest/otel.rs::apply_modified_otlp_delta_bytes`).
Per-platform Adapter implementations (the Layer-4 shims) translate
their host's native event into `Observation`, hand it to a
`Precompute`, and translate the runtime's emitted `SketchEnvelope`
back to the host's native event.

## Status — Phase 3 step 1: bootstrap

This PR is the **bootstrap**: types, traits, and basic struct
skeletons. The API surface mirrors `asap-precompute-go` 1:1 so the
runtime migration from `ASAPQuery-backend`'s ingest path lands
in subsequent PRs (Phase 3 step 2+) against a stable contract.

Runtime methods (`Precompute::observe`, `observe_envelope`,
`tick`, `drain`, `WindowState::*`, `SnapshotCache::compute_delta`)
are `unimplemented!()` and reference the Go file they migrate from.

## Module map

| Rust module | Go reference |
| --- | --- |
| `observation` | `observation.go` |
| `envelope` | `envelope.go` |
| `precompute` | `precompute.go` |
| `window` | `window.go` |
| `snapshot_cache` | `snapshot_cache.go` |
| `matchers` | `matchers.go` |
| `config` | `config.go` |
| `adapter` | `adapter.go` |
| `control_channel` | `controlchannel/channel.go` |

## Consuming from another Rust crate

This crate lives in-tree alongside the Go runtime so the two stay in
lockstep. `ASAPQuery-backend` already depends on `asap_sketchlib`
via a git URL; consumers pick the same approach for
`asap-precompute-rs`:

```toml
# Cargo.toml of a downstream crate
[dependencies]
asap-precompute-rs = { git = "https://github.com/ProjectASAP/ASAPCollector", branch = "main" }
```

For local development inside a checked-out tree, a path dependency
works:

```toml
[dependencies]
asap-precompute-rs = { path = "../ASAPCollector/asap-precompute-rs" }
```

This crate's own dependency on `asap_sketchlib` uses a path
dependency (`../../asap_sketchlib`) by default for local iteration;
mirror to the same git URL `ASAPQuery-backend` uses when wiring this
crate into a published artifact.

## Validation

```text
cd asap-precompute-rs
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

All four must pass. Tests today are type-level (constructors, trait
impls, serde round-trip); behavioral tests for the runtime
arrive with the migration in Phase 3 step 2.
