#!/usr/bin/env python3
"""
measure-baseline.py — pull per-baseline CPU, memory, bandwidth
figures from the running compose stack.

Queries Prometheus for agent / gateway / backend metrics, plus
supplements with `docker stats` for containers that don't
self-report (backend + the producer / fake-exporter, and — added
2026-05-05 for paper blocker #3 — every agent container, so the
raw / Gorilla / Serf baselines that don't emit
`otelcol_asapcollector_processor_*_bytes_total` still produce
on-the-wire bandwidth figures). Producer-side
columns were added 2026-04-23 to support the three-axis SDK
aggregation sweep — see
docs/sdk-cost-evaluation.md and the encoding-axis
ablation in particular, which measures the cost the SDK pays to
emit raw / full-sketch / delta-sketch per tick.

Column-by-column provenance is documented in
`docs/eval-instrumentation-notes.md`. Briefly:

  * `agent_in_kib_per_s` / `agent_out_kib_per_s` — sketch
    baselines source these from the patched processor's
    `otelcol_asapcollector_processor_input_bytes_total` /
    `…_output_bytes_total` (in-process protobuf bytes). Raw +
    Gorilla baselines have no such counter so the script falls
    back to `docker stats` net rx / tx for the agent container,
    which is the on-the-wire rate the bandwidth claim actually
    cares about. The two paths are NOT identical — see the doc.
  * `backend_samples_per_s` — primary path is
    `rate(asap_ingest_samples_total[…])` from the backend's
    own /metrics; if the backend image doesn't expose that
    counter (current asap/query-backend:dev does not), falls
    back to `rate(otelcol_exporter_sent_metric_points_total
    {job="gateway"})`, which is the points the gateway forwarded
    to the backend (= ingest rate).
  * `backend_query_p99_ms` — primary path is the backend's own
    `asap_query_duration_seconds` Prom histogram. When that's
    absent OR `--replay-jsonl PATH` is passed, falls back to
    the client-side replay JSONL so the stat survives `docker
    compose down -v` after the cell.

Output is CSV to stdout. The intended flow is:

    ./run-baseline-sweep.sh > sweep-YYYYMMDD.csv

which iterates baseline × (rate, cardinality) and concatenates
measure-baseline.py output from each configuration.

This is a pure stdlib Python script — no extra deps.

Usage:
    python3 measure-baseline.py [--prom URL] [--window 60s]
        [--baseline b0-raw|b1-serf|b2-full|b3-delta|b4-tunable|b5-gorilla]
        [--bytes-sample-window 5s]
        [--replay-jsonl PATH]
        [--agent-container-prefix docker-compose-agent-]

The `--baseline` tag is just pass-through labelling; the script
doesn't know or care which baseline is active.
"""
import argparse
import csv
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from typing import Any


def _is_nan(x: float) -> bool:
    """NaN-safe `x != x`. Avoids importing math just for this."""
    return x != x  # noqa: PLR0124  — intentional NaN check


def prom_query(url: str, q: str) -> list[dict[str, Any]]:
    """Instant query; returns the `result` list."""
    params = urllib.parse.urlencode({"query": q})
    with urllib.request.urlopen(f"{url}/api/v1/query?{params}", timeout=5) as resp:
        body = json.load(resp)
    if body.get("status") != "success":
        raise RuntimeError(f"Prometheus error: {body}")
    return body["data"]["result"]


def scalar(url: str, q: str) -> float:
    """Single-valued query → float, or NaN if no result."""
    r = prom_query(url, q)
    if not r:
        return float("nan")
    return float(r[0]["value"][1])


def _parse_size_to_bytes(s: str) -> float:
    """'1.23kB' / '4.56MB' / '1.0GB' → bytes. NaN on parse error."""
    s = s.strip()
    if not s:
        return float("nan")
    # Units docker uses for NetIO: B, kB, MB, GB (decimal SI).
    for suffix, factor in (("GB", 1e9), ("MB", 1e6), ("kB", 1e3), ("B", 1.0)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    # Fallback: raw number
    try:
        return float(s)
    except ValueError:
        return float("nan")


def _parse_mem_to_mib(s: str) -> float:
    """'335.8MiB' / '1.0GiB' / '512KiB' → MiB. NaN on parse error."""
    s = s.strip()
    for suffix, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1.0 / 1024)):
        if s.endswith(suffix):
            try:
                return float(s[: -len(suffix)]) * factor
            except ValueError:
                return float("nan")
    return float("nan")


def docker_stats() -> dict[str, dict[str, float]]:
    """
    `docker stats --no-stream` keyed by container name.

    Returns a dict per container:
        {
            "cpu_pct":     float,
            "mem_mib":     float,
            "net_rx_bytes": float,   # cumulative since container start
            "net_tx_bytes": float,   # cumulative since container start
        }

    NetIO is cumulative — to get a rate, call this twice and divide
    the delta by the wall-clock gap (see docker_bytes_rate).
    """
    out = subprocess.check_output(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.NetIO}}",
        ],
        text=True,
    )
    result: dict[str, dict[str, float]] = {}
    for line in out.strip().splitlines():
        name, cpu, mem, netio = line.split("|", 3)
        cpu_pct = float(cpu.rstrip("%"))
        mem_mib = _parse_mem_to_mib(mem.split("/", 1)[0])
        # NetIO looks like "1.23kB / 4.56MB" = rx / tx
        if "/" in netio:
            rx_str, tx_str = [p.strip() for p in netio.split("/", 1)]
            net_rx = _parse_size_to_bytes(rx_str)
            net_tx = _parse_size_to_bytes(tx_str)
        else:
            net_rx = net_tx = float("nan")
        result[name] = {
            "cpu_pct": cpu_pct,
            "mem_mib": mem_mib,
            "net_rx_bytes": net_rx,
            "net_tx_bytes": net_tx,
        }
    return result


def docker_stats_with_bytes_rate(
    window_s: float,
) -> dict[str, dict[str, float]]:
    """
    Sample `docker stats` twice separated by `window_s` seconds.
    Returns per-container:
        {
            "cpu_pct":         float,  # from the second sample
            "mem_mib":         float,  # from the second sample
            "net_tx_per_s":    float,  # delta / window
            "net_rx_per_s":    float,
        }

    Adds `window_s` of wall-clock overhead to the caller; worth it
    because cAdvisor isn't in the compose stack (see
    deploy/configs/prometheus.yml — only OTel targets configured).
    """
    t0 = time.monotonic()
    s0 = docker_stats()
    time.sleep(window_s)
    t1 = time.monotonic()
    s1 = docker_stats()
    dt = max(t1 - t0, 1e-6)

    result: dict[str, dict[str, float]] = {}
    for name, cur in s1.items():
        prev = s0.get(name, {})
        result[name] = {
            "cpu_pct": cur["cpu_pct"],
            "mem_mib": cur["mem_mib"],
            "net_tx_per_s": (
                (cur["net_tx_bytes"] - prev.get("net_tx_bytes", cur["net_tx_bytes"]))
                / dt
            ),
            "net_rx_per_s": (
                (cur["net_rx_bytes"] - prev.get("net_rx_bytes", cur["net_rx_bytes"]))
                / dt
            ),
        }
    return result


# Prometheus query templates. `w` is the rate window (e.g. "1m").
#
# Both agent and gateway are on otelcol v0.141 today (the gateway was
# bumped along with the agent during the Phase 2 shim PRs); both use
# the `_total` suffix on counters and the `_bytes` suffix on the
# process-RSS gauge. The original measure-baseline.py was written
# against a v0.108 gateway and v0.141 agent — that asymmetry no
# longer holds, so the gateway queries now mirror the agent shape.
# Closes Phase 2.11B gap #1.
#
# Each gateway query is wrapped in `or` against the v0.108 (no-suffix)
# variant so this script keeps producing rows when run against a
# legacy gateway image (e.g. someone replaying an old worktree).
# When neither variant exists the result is NaN, same as before.
QUERIES: dict[str, str] = {
    # ── Agent tier (source) ─────────────────────────────────────
    "agent_cpu_cores": (
        "avg by (agent_id) ("
        "  rate(otelcol_process_cpu_seconds_total{{job=\"agents\"}}[{w}])"
        ")"
    ),
    "agent_rss_mib": (
        "avg by (agent_id) ("
        "  otelcol_process_memory_rss_bytes{{job=\"agents\"}} / 1024 / 1024"
        ")"
    ),
    # Bytes entering the agent's sketch pipeline (in-process
    # protobuf bytes from the patched processor self-monitor).
    # Only populated for the ASAP-patched processors
    # (DDSketch/HLL/KLL/etc.); B0/B1/B5 leave this NaN here and
    # fall back to the docker-stats agent net rx in main()
    # (paper blocker #3) — see docs/eval-instrumentation-notes.md
    # for the in-process-vs-wire-bytes caveat.
    "agent_in_kib_per_s": (
        "avg by (agent_id) ("
        "  sum by (agent_id) ("
        "    rate(otelcol_asapcollector_processor_input_bytes_total"
        "         {{job=\"agents\"}}[{w}])"
        "  )"
        ") / 1024"
    ),
    "agent_out_kib_per_s": (
        "avg by (agent_id) ("
        "  sum by (agent_id) ("
        "    rate(otelcol_asapcollector_processor_output_bytes_total"
        "         {{job=\"agents\"}}[{w}])"
        "  )"
        ") / 1024"
    ),
    # Receiver-level point rate is universal across baselines.
    # Useful as an input-side bandwidth proxy for B0/B1/B5.
    "agent_points_per_s": (
        "avg by (agent_id) ("
        "  rate(otelcol_receiver_accepted_metric_points_total"
        "       {{job=\"agents\"}}[{w}])"
        ")"
    ),
    # ── Gateway tier (destination-1) ────────────────────────────
    # v0.141 names with `_total` suffix; legacy v0.108 names appended
    # via PromQL `or` so old worktrees keep producing data.
    "gateway_cpu_cores": (
        "rate(otelcol_process_cpu_seconds_total{{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_process_cpu_seconds{{job=\"gateway\"}}[{w}])"
    ),
    "gateway_rss_mib": (
        "otelcol_process_memory_rss_bytes{{job=\"gateway\"}} / 1024 / 1024"
        " or otelcol_process_memory_rss{{job=\"gateway\"}} / 1024 / 1024"
    ),
    # `or vector(0)` ensures B1 / B5 baselines (drop_original=true,
    # so gateway sees zero traffic and the metric is never
    # initialised) produce 0 rather than NaN — bandwidth claim #1
    # cares about the difference between "the gateway is gone"
    # (NaN) and "this baseline structurally bypasses the gateway"
    # (0). Paper blocker #3.
    "gateway_points_per_s": (
        "rate(otelcol_receiver_accepted_metric_points_total"
        "     {{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_receiver_accepted_metric_points"
        "        {{job=\"gateway\"}}[{w}])"
        " or vector(0)"
    ),
    "gateway_out_series_per_s": (
        "rate(otelcol_exporter_sent_metric_points_total"
        "     {{job=\"gateway\"}}[{w}])"
        " or rate(otelcol_exporter_sent_metric_points"
        "        {{job=\"gateway\"}}[{w}])"
        " or vector(0)"
    ),
    # ── Backend tier (destination-2) ────────────────────────────
    # `asap_query_duration_seconds_bucket` populates only under an
    # ingest+query soak (an ingest-only soak leaves the histogram
    # empty → NaN). When `--replay-jsonl` is passed in main(),
    # `backend_query_p99_ms` is computed client-side and overrides
    # the Prom histogram (paper blocker #3, item 3).
    #
    # `asap_ingest_samples_total` is NOT exposed by the current
    # asap/query-backend:dev image — only query-side counters are.
    # Falls back to gateway-egress in `FALLBACK_QUERIES` below.
    "backend_samples_per_s": "rate(asap_ingest_samples_total[{w}])",
    "backend_query_p99_ms": (
        "1000 * histogram_quantile(0.99, "
        "sum by (le) (rate(asap_query_duration_seconds_bucket[{w}])))"
    ),
}


# ── Fallback queries (paper blocker #3) ─────────────────────────
#
# Used when the primary QUERIES counter is absent — current
# `asap/query-backend:dev` doesn't expose `asap_ingest_samples_total`
# or `asap_query_duration_seconds_bucket` even though the backend
# /metrics surface is reachable, so the primary path returns NaN.
# These fallbacks let us populate the columns from signals that
# DO exist:
#
#   gateway → backend point rate is what the backend ingests
#   (the gateway's exporter counter == the backend's receiver
#   accepted-points counter, modulo dropped batches which are
#   themselves an interesting signal but tracked elsewhere).
#
# The query-latency fallback is the client-side replay JSONL,
# applied in main() because it isn't a Prom query.
FALLBACK_QUERIES: dict[str, str] = {
    "backend_samples_per_s": (
        "rate(otelcol_exporter_sent_metric_points_total"
        "     {{exporter=~\"otlp.*backend.*|otlp/backend\","
        "      job=\"gateway\"}}[{w}])"
        " or rate(otelcol_exporter_sent_metric_points"
        "        {{exporter=~\"otlp.*backend.*|otlp/backend\","
        "         job=\"gateway\"}}[{w}])"
        " or vector(0)"
    ),
}


def _client_p99_from_jsonl(path: str) -> float:
    """Compute p99 of `duration_ms` over successful queries in
    the replay JSONL output. Returns NaN if the file is missing,
    empty, or has no successful rows. Stdlib-only — not using
    `statistics.quantiles` because we want strict p99 (last entry
    of the 99th percentile bin) on small samples too.

    "Success" here means `status == "success"` AND `http_code`
    is 2xx — covers the "Prometheus said OK but body said error"
    case where http is 200 but the JSON status is "error". The
    e2e `promql_replay.py` writes one JSON object per line.
    """
    durs: list[float] = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("status") != "success":
                    continue
                code = rec.get("http_code")
                if code is not None and not (200 <= int(code) < 300):
                    continue
                d = rec.get("duration_ms")
                if isinstance(d, (int, float)):
                    durs.append(float(d))
    except FileNotFoundError:
        return float("nan")
    except Exception as e:
        print(f"# replay-jsonl read failed ({path}): {e}", file=sys.stderr)
        return float("nan")

    if not durs:
        return float("nan")
    durs.sort()
    # Nearest-rank p99 — matches what the backend's Prom histogram
    # would produce in the limit. For small N (≤100) this just
    # returns the max, which is fine: the e2e replay drives ≥300
    # queries per cell at the default qps=5 / soak=60s.
    idx = max(0, min(len(durs) - 1, int(round(0.99 * (len(durs) - 1)))))
    return durs[idx]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--prom", default="http://localhost:9090")
    p.add_argument(
        "--window",
        default="2m",
        help="rate() lookback window. ≥2m needed when baselines emit on "
        "60s windows — otherwise rate() sees a single sample and returns NaN.",
    )
    p.add_argument("--baseline", default="unset", help="label for the output row")
    p.add_argument(
        "--scale", default="N1", help="label for the scale (e.g. N1 / N10)"
    )
    p.add_argument("--rate", default="", help="workload rate label")
    p.add_argument("--cardinality", default="", help="workload cardinality label")
    p.add_argument(
        "--bytes-sample-window",
        type=float,
        default=15.0,
        help="seconds between the two docker-stats samples used to compute "
        "producer_bytes_out_per_s. Too short and the tx counter barely moves; "
        "too long and the sweep gets expensive per baseline. Bumped to 15 s "
        "from 5 s on 2026-05-06 to close the HLL N=1 NaN gap (4 of 12 HLL "
        "cells in the 60-cell sweep had agent_cpu / rss / in / out = NaN "
        "because the 5 s window landed between agent flushes during cell "
        "teardown).",
    )
    p.add_argument(
        "--bytes-sample-warmup",
        type=float,
        default=0.0,
        help="If >0, take an extra `docker stats` sample this many "
        "seconds before the first measurement window and discard it. "
        "Pairs with --bytes-sample-window: a freshly-restarted "
        "container's first `docker stats` snapshot can include a "
        "skewed cumulative counter (low rx/tx because the container "
        "hasn't yet flushed its first batch); the warm-up evicts that "
        "stale snapshot so the first delta is over a fully-running "
        "container.",
    )
    p.add_argument(
        "--producer-container",
        default="docker-compose-fake-exporter-1",
        help="docker container name of the producer to scrape for SDK-side "
        "CPU / RSS / bytes-out.",
    )
    p.add_argument(
        "--agent-container-prefix",
        default="docker-compose-agent-",
        help="docker container name prefix used to identify agent containers "
        "for the wire-bytes fallback. Average of net rx/tx across matching "
        "containers feeds agent_in_kib_per_s / agent_out_kib_per_s when the "
        "patched-processor counters aren't emitted (raw / Gorilla / Serf "
        "baselines). Set to empty to disable the fallback.",
    )
    p.add_argument(
        "--replay-jsonl",
        default="",
        help="path to promql_replay.py JSONL output. When set, "
        "backend_query_p99_ms is computed client-side (p99 of "
        "successful-query duration_ms) — survives the `docker compose "
        "down -v` that follows each sweep cell. Falls through to the "
        "Prom histogram path when empty.",
    )
    args = p.parse_args()

    # Prom-sourced metrics. `.format(w=…)` fills the rate window.
    rows: dict[str, float] = {}
    for key, tmpl in QUERIES.items():
        q = tmpl.format(w=args.window)
        try:
            # Per-agent metrics produce multiple rows — average them
            # for the single-row CSV output.
            vals = [float(r["value"][1]) for r in prom_query(args.prom, q)]
            rows[key] = sum(vals) / len(vals) if vals else float("nan")
        except Exception as e:
            print(f"# query {key} failed: {e}", file=sys.stderr)
            rows[key] = float("nan")

    # Fallback Prom queries — only consulted when the primary
    # column is NaN. Keeps the primary path canonical (when it
    # works it's the most direct signal) and degrades gracefully
    # to the cross-baseline-universal counter when it doesn't.
    for key, tmpl in FALLBACK_QUERIES.items():
        if not _is_nan(rows.get(key, float("nan"))):
            continue
        q = tmpl.format(w=args.window)
        try:
            vals = [float(r["value"][1]) for r in prom_query(args.prom, q)]
            if vals:
                rows[key] = sum(vals) / len(vals)
                print(
                    f"# fallback {key}: {rows[key]:.3f} (from gateway exporter)",
                    file=sys.stderr,
                )
        except Exception as e:
            print(f"# fallback query {key} failed: {e}", file=sys.stderr)

    # docker stats — backend CPU/RSS + producer CPU / RSS / tx-bytes
    # + agent net rx/tx as fallback for agent_in/out_kib_per_s on
    # baselines that don't emit the patched-processor counters.
    # Single two-sample pass (separated by --bytes-sample-window)
    # avoids three trips through `docker stats`.
    #
    # Optional warm-up: take + discard one snapshot before the
    # measurement pair, so the first sample isn't taken at t=0
    # of a freshly-started container (whose net rx/tx counters
    # haven't yet seen a flush). HLL N=1 cells in the 60-cell
    # sweep showed agent_cpu/rss/in/out = NaN because the 5 s
    # measurement window landed in a flush-quiet interval; the
    # warm-up + 15 s window combination closes that gap.
    try:
        if args.bytes_sample_warmup > 0:
            try:
                _ = docker_stats()
                time.sleep(args.bytes_sample_warmup)
                _ = docker_stats()
            except Exception as e:
                print(f"# bytes-sample-warmup probe failed: {e}", file=sys.stderr)
        stats = docker_stats_with_bytes_rate(args.bytes_sample_window)

        backend = stats.get("docker-compose-backend-1") or {}
        rows["backend_cpu_pct"] = backend.get("cpu_pct", float("nan"))
        rows["backend_rss_mib"] = backend.get("mem_mib", float("nan"))

        producer = stats.get(args.producer_container) or {}
        # docker reports CPU as a percentage of one core; scale to
        # cores so the column matches agent_cpu_cores.
        rows["producer_cpu_cores"] = producer.get("cpu_pct", float("nan")) / 100.0
        rows["producer_rss_mib"] = producer.get("mem_mib", float("nan"))
        rows["producer_bytes_out_per_s"] = producer.get(
            "net_tx_per_s", float("nan")
        )

        # Agent net rx/tx fallback. Average across all containers
        # whose names start with --agent-container-prefix. The
        # patched-processor self-monitor counter measures
        # in-process protobuf bytes (different from the wire
        # bytes), so even when both signals exist they will not
        # match. We prefer the patched counter when available
        # because it cleanly attributes bytes to the sketch
        # processor; the docker-stats fallback is the universal
        # "what bandwidth claim #1 actually wants" fallback.
        if args.agent_container_prefix:
            agent_rx = []
            agent_tx = []
            for name, vals in stats.items():
                if not name.startswith(args.agent_container_prefix):
                    continue
                rx = vals.get("net_rx_per_s", float("nan"))
                tx = vals.get("net_tx_per_s", float("nan"))
                if not _is_nan(rx):
                    agent_rx.append(rx)
                if not _is_nan(tx):
                    agent_tx.append(tx)
            if _is_nan(rows.get("agent_in_kib_per_s", float("nan"))) and agent_rx:
                rows["agent_in_kib_per_s"] = (sum(agent_rx) / len(agent_rx)) / 1024.0
                print(
                    f"# fallback agent_in_kib_per_s: {rows['agent_in_kib_per_s']:.3f} "
                    f"(from docker stats net rx, n={len(agent_rx)})",
                    file=sys.stderr,
                )
            if _is_nan(rows.get("agent_out_kib_per_s", float("nan"))) and agent_tx:
                rows["agent_out_kib_per_s"] = (sum(agent_tx) / len(agent_tx)) / 1024.0
                print(
                    f"# fallback agent_out_kib_per_s: {rows['agent_out_kib_per_s']:.3f} "
                    f"(from docker stats net tx, n={len(agent_tx)})",
                    file=sys.stderr,
                )
    except Exception as e:
        print(f"# docker stats failed: {e}", file=sys.stderr)
        for k in (
            "backend_cpu_pct",
            "backend_rss_mib",
            "producer_cpu_cores",
            "producer_rss_mib",
            "producer_bytes_out_per_s",
        ):
            rows[k] = float("nan")

    # Client-side p99 fallback. Always wins over the Prom path
    # when --replay-jsonl is set, because client-side latency is
    # what the caller actually experienced and survives stack
    # teardown. Prom path is the secondary fallback for the
    # legacy `run-baseline-sweep.sh` flow which doesn't drive
    # queries.
    if args.replay_jsonl:
        client_p99 = _client_p99_from_jsonl(args.replay_jsonl)
        if not _is_nan(client_p99):
            rows["backend_query_p99_ms"] = client_p99
            print(
                f"# backend_query_p99_ms (client-side): {client_p99:.3f} ms "
                f"from {args.replay_jsonl}",
                file=sys.stderr,
            )

    # Emit CSV with a stable column order. Producer columns sit
    # between the label columns and the agent columns so they show
    # up first in spreadsheets — the three-axis sweep's headline
    # metric is producer_bytes_out_per_s.
    header = [
        "baseline",
        "scale",
        "rate",
        "cardinality",
        "producer_cpu_cores",
        "producer_rss_mib",
        "producer_bytes_out_per_s",
        "agent_cpu_cores",
        "agent_rss_mib",
        "agent_in_kib_per_s",
        "agent_out_kib_per_s",
        "agent_points_per_s",
        "gateway_cpu_cores",
        "gateway_rss_mib",
        "gateway_points_per_s",
        "gateway_out_series_per_s",
        "backend_cpu_pct",
        "backend_rss_mib",
        "backend_samples_per_s",
        "backend_query_p99_ms",
    ]
    w = csv.writer(sys.stdout)
    w.writerow(header)
    w.writerow(
        [
            args.baseline,
            args.scale,
            args.rate,
            args.cardinality,
            *[f"{rows.get(c, float('nan')):.3f}" for c in header[4:]],
        ]
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
