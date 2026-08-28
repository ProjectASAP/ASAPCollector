# Build, test, and patch workflow

## Code architecture

ASAPCollector combines repository-owned components with pinned OpenTelemetry
submodules. Changes to upstream OpenTelemetry trees are maintained as tracked
overlays:

| Area | Maintained source | Build target |
| --- | --- | --- |
| Collector core | `opentelemetry-collector-patch/` | `opentelemetry-collector/` |
| Collector components | `opentelemetry-collector-contrib-patch/` | `opentelemetry-collector-contrib/` |
| Go SDK | `opentelemetry-go-patch/` | `opentelemetry-go/` |
| OTLP schema | `opentelemetry-proto-patch/` | `opentelemetry-proto/` |
| Precompute contracts | `asap-precompute-go/`, `asap-precompute-rs/` | linked into collector/backend builds |

Edit the maintained source, not only the generated submodule worktree. The
restore scripts copy overlays into the pinned upstream checkout; backup scripts
copy intentional upstream-tree edits back into the overlay.

## Prepare the checkout

Initialize the MVP submodules and prerequisites once:

```bash
./setup.sh --no-go
```

Omit `--no-go` when the setup script should install the repository's Go
toolchain. `asap-otel` also resolves sibling checkouts of `sketchlib-go` during
the final build.

## Build the collector distribution

```bash
./build_asap_otel.sh
```

The script restores overlays, selects OpenTelemetry Collector Builder v0.141.0,
generates the distribution, adds local module replacements, and writes:

```text
opentelemetry-collector-contrib-patch/cmd/asap-otel/asap-otel
```

For an incremental build after overlays are already restored:

```bash
./build_asap_otel.sh --skip-patches
```

## Focused verification

Run tests from the module that owns the change. There is no single root Go
module.

```bash
(cd asap-precompute-go && go test ./...)
(cd asap-gorilla-go && go test ./...)
(cd asap-precompute-rs && cargo test)
```

For a collector processor change, restore the overlay and test its module under
`opentelemetry-collector-contrib/` or the generated `asap-otel` module. For a
wire-format change, regenerate the relevant protobuf bindings and verify both Go
and Rust decoders before treating the change as compatible.

The end-to-end harness checks integration behavior:

```bash
python3 -m unittest discover \
  -s deploy/mvp-multinode/scripts/tests -p 'test_*.py'
bash -n deploy/mvp-multinode/scripts/run_demo.sh
```

## Before committing

Confirm that intentional OpenTelemetry changes exist in the corresponding
tracked patch directory, generated binaries are untracked, and submodule commit
pointers changed only when an upstream pin change was intended. Then run the
smallest relevant unit suite plus the integration checks affected by the change.

See [OpAMP configuration push](opamp-config-push.md) for the public plan contract
and the [MVP demo runbook](../user_guide/mvp-demo-runbook.md) for system evidence.
