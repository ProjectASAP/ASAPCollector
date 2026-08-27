# Future support

This directory contains integrations that are intentionally outside the
current OpenTelemetry MVP acceptance path:

- `telegraf/` — Telegraf submodule, patches, maintenance helpers, and local
  benchmarks.
- `otap/` — OTAP/Arrow submodule, patches, and builder/image.

The MVP does not initialize, build, or deploy these components. Keep changes
here isolated until a separately scoped Telegraf or OTAP milestone is started.
