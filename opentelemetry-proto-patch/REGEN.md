# Regenerating the patched OTLP Go bindings

The ASAP-patched `metrics.proto` adds five sketch message types (`DDSketch`,
`KLLSketch`, `CountSketch`, `CountMinSketch`, `HLLSketch`) plus their
`*DataPoint` and `*Encoding` enums on top of upstream
[`opentelemetry-proto` v1.9.0]. The Go bindings under
`gen/go/go.opentelemetry.io/proto/otlp/` are committed to this repo so the
fake-exporter / asap-otel Docker builds don't have to reinvent a protoc
toolchain at build time. The `replace` directives in
`opentelemetry-go-patch/exporters/otlp/otlpmetric/{otlpmetricgrpc,otlpmetrichttp}/go.mod`
and `deploy/fake-exporter/go.mod` point at this gen tree.

## When to regenerate

Whenever any `.proto` file under `opentelemetry-proto-patch/opentelemetry/`
changes. Both the patched files (carried in this directory) AND any
upstream-only files transitively imported by them
(`common.proto`, `resource.proto`, etc.) must be passed to `protoc`.

## Recipe

The repo pins upstream proto to `v1.9.0` (commit
`a8951735f7801e8adfaec5c0ace9262771cfec6e`). The recipe overlays this
patch's `.proto` files on top of a clean v1.9.0 checkout, then runs
`protoc-gen-go` (with `plugins=grpc`) per upstream's Makefile target
`gen-go`.

```bash
# 1. Get a clean v1.9.0 of upstream opentelemetry-proto.
PROTO_BUILD=/tmp/proto-build
rm -rf "$PROTO_BUILD"
git clone --depth 1 --branch v1.9.0 \
    https://github.com/open-telemetry/opentelemetry-proto.git "$PROTO_BUILD"

# 2. Overlay the patched .proto files.
REPO_ROOT="$(git rev-parse --show-toplevel)"
cp "$REPO_ROOT/opentelemetry-proto-patch/opentelemetry/proto/metrics/v1/metrics.proto" \
   "$PROTO_BUILD/opentelemetry/proto/metrics/v1/metrics.proto"
cp "$REPO_ROOT/opentelemetry-proto-patch/opentelemetry/proto/collector/metrics/v1/metrics_service.proto" \
   "$PROTO_BUILD/opentelemetry/proto/collector/metrics/v1/metrics_service.proto"

# 3. Run protoc-gen-go (with grpc plugin) via the otel/build-protobuf
#    image — the same image upstream uses for `make gen-go`.
cd "$PROTO_BUILD"
rm -rf gen/go && mkdir -p gen/go
for f in $(find opentelemetry/proto -name '*.proto'); do
    docker run --rm -u "$(id -u)" -v "${PWD}:${PWD}" -w "${PWD}" \
        otel/build-protobuf:0.9.0 \
        --proto_path="${PWD}" \
        --go_out=plugins=grpc:./gen/go \
        "$f"
done

# 4. Copy the generated tree back into the patch repo.
rm -rf "$REPO_ROOT/opentelemetry-proto-patch/gen/go/go.opentelemetry.io/proto/otlp"/*
cp -r gen/go/go.opentelemetry.io/proto/otlp/* \
      "$REPO_ROOT/opentelemetry-proto-patch/gen/go/go.opentelemetry.io/proto/otlp/"

# 5. Confirm the new sketch types are present:
grep -c "DDSketchDataPoint\|KLLSketchDataPoint\|CountSketchDataPoint\|CountMinSketchDataPoint\|HLLSketchDataPoint" \
    "$REPO_ROOT/opentelemetry-proto-patch/gen/go/go.opentelemetry.io/proto/otlp/metrics/v1/metrics.pb.go"
```

The `gen/go/go.opentelemetry.io/proto/otlp/go.mod` is hand-maintained
(modeled on upstream) and pins protobuf / gRPC / grpc-gateway versions
that match `deploy/fake-exporter/go.sum`. Update it only when those pins
shift (rare).

## Build wiring

`restore_otel_proto_patches.sh` (in the repo root) copies this entire
directory tree, including `gen/go/...`, on top of the
`opentelemetry-proto/` submodule. The Docker build for fake-exporter
copies `opentelemetry-proto/` into the build context, and
`deploy/fake-exporter/go.mod` has a
`replace go.opentelemetry.io/proto/otlp => ../../opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp`
that picks up the regenerated bindings.

## Why the bindings are committed (not generated at build time)

- The Docker build runs `golang:1.25-bookworm` (no protoc, no Docker-in-
  Docker for the otel/build-protobuf image). Pulling the toolchain
  inside the build would add 200+ MB of layers and a Docker socket
  mount. Easier to commit ~10 KLOC of generated `.pb.go`.
- Upstream `opentelemetry-proto` itself commits its `gen/...` outputs,
  so we follow the same pattern.
