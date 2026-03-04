# DataCollector

## Metrics Collection Pipeline Deployments

<table>
  <caption style="caption-side: top; text-align: center;"><strong>Table 1: Example deployment options</strong></caption>
  <thead>
    <tr>
      <th>Deployment stage / Example pipeline</th>
      <th>Instrumentation</th>
      <th>Agent aggregation (optional)</th>
      <th>Gateway aggregation (optional)</th>
      <th>Backend databases / storage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Prometheus scrape pipeline</td>
      <td>Prometheus client libraries</td>
      <td>OpenTelemetry Collector (agent mode scraping or Prometheus remote_write)</td>
      <td>OpenTelemetry Collector (gateway mode for routing, batching, export fan-out)</td>
      <td rowspan="3">
        Prometheus TSDB / Thanos / Mimir<br/>
        InfluxDB / IOx / VictoriaMetrics<br/>
        TimescaleDB / ClickHouse / BigQuery / other OTLP-compatible sinks
      </td>
    </tr>
    <tr>
      <td>Telegraf + Influx pipeline</td>
      <td>Influx Line Protocol emitters</td>
      <td>Telegraf Agent (local metric/log aggregation)</td>
      <td>
        OpenTelemetry Collector<br/>
        <small>(via ILP receiver if available, or via intermediary like Kafka/HTTP → OTLP)</small>
      </td>
    </tr>
    <tr>
      <td>Native OTLP pipeline</td>
      <td>OpenTelemetry SDK (OTLP exporters)</td>
      <td>OpenTelemetry Collector (sidecar or daemonset agent)</td>
      <td>OpenTelemetry Collector (centralized gateway: auth, routing, tail-sampling)</td>
    </tr>
  </tbody>
</table>

<table>
  <caption style="caption-side: top; text-align: center;"><strong>Table 2: All Possible Deployment Combinations</strong></caption>
  <thead>
    <tr>
      <th>Instrumentation</th>
      <th>Local agent </th>
      <th>Gateway (optional)</th>
      <th>Backend storage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>
         &bull; Prometheus client libraries<br/>
         &bull; OpenTelemetry SDK<br/>
         &bull; Influx Line Protocol emitters<br/>
      </td>
      <td>
        &bull; OpenTelemetry Collector (agent mode scraping)<br/>
        &bull; Telegraf scraping Prometheus endpoints<br/>
        &bull; Kafka<br/>
      </td>
      <td>
        OpenTelemetry Collector<br/>
        <small>Routing, auth, tail-sampling, multi-backend fan-out</small>
      </td>
      <td>
        Prometheus TSDB / Thanos / Mimir / InfluxDB / IOx / VictoriaMetrics / TimescaleDB / ClickHouse / BigQuery / OTLP sinks
      </td>
    </tr>
  </tbody>
</table>

## Cloning

Clone with submodules on first checkout so the embedded Telegraf tree is pulled
automatically:

```bash
git clone --recurse-submodules git@github.com:ProjectASAP/DataCollector.git
# or
git clone --recurse-submodules https://github.com/ProjectASAP/DataCollector.git
```

If you have an existing clone, pull down the submodule once:

```bash
cd DataCollector
git submodule update --init --recursive
```

## Working with the Telegraf submodule

1. Enter the vendored repo and install Go dependencies:

   ```bash
   cd telegraf
   go mod tidy
   ```

2. Build or test Telegraf just like the upstream project:

   ```bash
   make telegraf   # or `make test`
   ```

3. When upstream Telegraf updates are needed, pull them into the submodule:

   ```bash
   git submodule update --remote telegraf
   ```

#### Code organization
```
telegraf/                     # upstream InfluxData Telegraf checkout
telegraf-patch/               # tracked overlay of our custom Telegraf changes
telegraf_benchmarks/          # local benchmark harness that uses the submodule
backup_telegraf_patches.sh    # copies modified submodule files into telegraf-patch/
restore_telegraf_patches.sh   # reapplies tracked patches back into telegraf/
```

Use the restore → edit → backup flow here as well:
1. Run `./restore_telegraf_patches.sh` after cloning or resetting the submodule.
2. Hack and test directly inside `telegraf/`.
3. Run `./backup_telegraf_patches.sh` so the overlay captures every modified file
   before committing.

## Working with the OpenTelemetry submodule

#### Code organization
```
opentelemetry-go/                       # upstream Go SDK submodule
opentelemetry-collector/                # upstream collector core checkout
opentelemetry-collector-contrib/        # upstream contrib components
opentelemetry-proto/                    # upstream OTLP proto definitions

opentelemetry-go-patch/                 # tracked overlay of our SDK changes
opentelemetry-collector-patch/          # tracked overlay of collector-core tweaks
opentelemetry-collector-contrib-patch/  # tracked overlay of contrib-only tweaks
opentelemetry-proto-patch/              # tracked overlay of proto changes

backup_otel_*.sh / restore_otel_*.sh    # helper scripts that sync overlays <-> submodules
```

#### Workflow

1. After cloning (or whenever submodules are reset), run `./restore_otel_patches.sh`
   from the repo root. This copies the tracked overlay files into their matching
   submodules so our custom code is available locally.
2. Make changes inside the actual submodule directories (for example,
   `opentelemetry-collector/...`). Build, test, and iterate directly against the
   upstream layout.
3. Before committing, run `./backup_otel_patches.sh`. The script copies every
   modified file reported by `git status` inside the submodules into the
   corresponding `*-patch/` overlay, which is what we check in.
4. Repeat the restore → edit → backup cycle whenever upstream commits are pulled
   in via `git submodule update --remote` so that local patches are always
   reapplied cleanly.

## Build and Test Cheat Sheet

### opentelemetry-proto
```bash
cd opentelemetry-proto
make gen-go
```

// Create a module descriptor in opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp if not exists. 
```bash
cd DataCollector/
cat <<'EOF' > opentelemetry-proto/gen/go/go.opentelemetry.io/proto/otlp/go.mod
module go.opentelemetry.io/proto/otlp

go 1.24
EOF
```

### Collector core (opentelemetry-collector)
1. Regenerate protobuf + pdata after touching proto specs:
   ```bash
   cd opentelemetry-collector
   make genproto
   make genpdata
   ```
2. Build the opentelemetry-collector binary:
   ```bash
   make otelcol
   ```
3. Run Go tests:
   ```bash
   go test ./... -count=1
   ```

### Collector-contrib (opentelemetry-collector-contrib)
Install OTel-Builder:
```bash
go install go.opentelemetry.io/collector/cmd/builder@v0.141.0
```

DDSketch processor example:
```bash
cd DataCollector/opentelemetry-collector-contrib
builder --config ./cmd/ddsketchcol/builder-config.yaml
```

### Go SDK / exporters (opentelemetry-go)
1. Format and tidy after edits:
   ```bash
   gofmt -w ./...
   go mod tidy
   ```
2. Run tests (ensure this repo is loaded as the module root):
   ```bash
   go test ./...
   ```
3. To mirror changes into the tracked overlay, copy updated files into
   `opentelemetry-go-patch/` before committing.

### opentelemetry-app
```bash
cd opentelemetry-app
go build ./cmd/fakemetricload    # produces ./fakemetricload
# or to run in-place:
go run ./cmd/fakemetricload
```
