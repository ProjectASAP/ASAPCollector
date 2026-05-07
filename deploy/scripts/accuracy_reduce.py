#!/usr/bin/env python3
"""Accuracy reducer (P8 — Phase-6 Fix 1: ground truth via Gorilla archive).

For each replay-client per-query row, the reducer obtains TWO answers
for the same PromQL string:

  1. **Warm answer** (sketch, from `SimpleEngine`) — already captured
     in `replay.jsonl` by the replay client (this is the demo's
     under-test answer).
  2. **Archive answer** (exact, from `GorillaQueryEngine`) — the
     ground truth, fetched by re-issuing the same query against the
     backend with the `X-ASAP-Engine: gorilla_archive` header set so
     the dispatcher bypasses `BackendStorageRouting` and queries the
     Gorilla chunks on MinIO directly.

The Gorilla archive on MinIO IS exact retention — querying it for
"the answer" is apples-to-apples with the warm sketch answer for the
same PromQL string. This:

  - removes the need for any new gateway raw-tee exporter
  - reuses the existing infrastructure (gorillas3processor +
    `GorillaQueryEngine`)
  - validates both engines simultaneously (cross-checks the warm
    sketch answer against the archive's exact answer)
  - aligns with the JSONL deprecation design (see
    `docs/design-jsonl-deprecation-and-gorilla-promql-completeness.md`)

Per replay row the reducer emits:

    cell, kind, query, t, duration_ms, plan_id, warm_answer,
    archive_answer, rel_err, recall, archive_query_latency_ms,
    archive_status, n_chunks_read, n_truth_samples

Status values for `archive_status`:

    ok            — archive engine answered, rel-err computed
    archive_miss  — archive engine returned no result (chunk hadn't
                    landed yet, or the metric isn't in the archive
                    at all). `archive_answer = NaN`, `rel_err = NaN`,
                    NOT a fatal error.
    archive_error — archive engine returned 4xx/5xx. Same NaN
                    treatment but flagged separately so the report
                    can call out "broken plumbing" vs "data not yet
                    landed".
    parse_skip    — replay row's query shape isn't one we recognise
                    (legacy: same as v1 reducer's silent skip).

Computes:

  - quantile     → relative error vs archive's exact P-th quantile
                   ε = |answer - truth| / max(truth, 1)
  - topk         → recall vs archive's exact top-K by sum(value)
                   recall = |sketch ∩ truth| / K
  - count_unique → relative error vs archive's exact distinct
                   cardinality. ε = |answer - truth| / max(truth, 1)
  - sum          → relative error vs archive's exact sum. Identity
                   check; any non-zero ε flags a bug.

Backwards-compat: `--use-jsonl` flag retained during Fix-1 transition.
When set, the reducer falls back to reading the per-cell `cold-truth/`
JSONL stream produced by the (deprecated) raw_tee. The default path
queries the backend's archive engine.

Usage (per cell, default archive-truth path):

  python3 accuracy_reduce.py \\
      --cell-dir /tmp/sweep/ddsketch_N1_w100ms_c10000 \\
      --backend  http://localhost:19091 \\
      --out      /tmp/sweep/ddsketch_N1_w100ms_c10000/accuracy.csv

Or in batch mode over a sweep root:

  python3 accuracy_reduce.py \\
      --sweep-root /tmp/sweep \\
      --backend    http://localhost:19091 \\
      --out        /tmp/sweep/all.csv

Or (transition-only) re-run against the legacy JSONL ground-truth tee:

  python3 accuracy_reduce.py \\
      --cell-dir /tmp/sweep/ddsketch_N1_w100ms_c10000 \\
      --use-jsonl \\
      --out      /tmp/sweep/ddsketch_N1_w100ms_c10000/accuracy.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from typing import Iterable


# Engine override surface mirrored on the backend HTTP handler. The
# `X-ASAP-Engine` header bypasses `BackendStorageRouting` and
# dispatches the query straight to the named engine (Phase-6 Fix 1).
ENGINE_OVERRIDE_HEADER = "X-ASAP-Engine"
DEFAULT_ARCHIVE_ENGINE_ID = "gorilla_archive"
DEFAULT_BACKEND_URL = "http://localhost:19091"
DEFAULT_ARCHIVE_TIMEOUT_S = 15.0


# --- ground-truth via Gorilla archive query ------------------------


class ArchiveTruthClient:
    """Re-issues the replay's PromQL string against the backend with
    the `X-ASAP-Engine: gorilla_archive` header so the dispatcher
    sends the query straight to the archive engine. Returns the same
    result-shape the replay client captured for the warm-tier path,
    plus the wall-clock duration for cost accounting.

    Tolerates archive misses (chunk hasn't landed yet) — emits a
    structured `archive_miss` outcome rather than crashing. Tolerates
    backend 4xx/5xx with a separate `archive_error` outcome so the
    final report can distinguish "broken plumbing" from "data not
    yet landed".
    """

    def __init__(
        self,
        backend_url: str,
        engine_id: str = DEFAULT_ARCHIVE_ENGINE_ID,
        timeout_s: float = DEFAULT_ARCHIVE_TIMEOUT_S,
    ) -> None:
        self.backend_url = backend_url.rstrip("/")
        self.engine_id = engine_id
        self.timeout_s = timeout_s

    def query(self, promql: str, t: float | None = None) -> dict:
        """Run `promql` against the archive engine. Returns a dict
        with keys: `status` ("ok" / "archive_miss" / "archive_error"),
        `result` (Prometheus instant-vector), `latency_ms` (float),
        `error` (Optional[str]), `n_chunks_read` (Optional[int]; the
        backend's S3-cost counter doesn't surface per-query, so this
        is None today)."""
        params = {"query": promql}
        if t is not None:
            params["time"] = str(t)
        url = f"{self.backend_url}/api/v1/query?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(
            url,
            headers={ENGINE_OVERRIDE_HEADER: self.engine_id},
        )
        started = time.perf_counter()
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_s) as resp:
                code = resp.getcode()
                body = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            latency_ms = (time.perf_counter() - started) * 1000.0
            # 404 = capability miss → treat as `archive_miss` (the
            # archive engine couldn't serve this query shape; not a
            # plumbing bug). 4xx/5xx other than 404 are flagged as
            # `archive_error`.
            status = "archive_miss" if e.code == 404 else "archive_error"
            return {
                "status": status,
                "result": None,
                "latency_ms": latency_ms,
                "error": f"http {e.code}: {e.reason}",
                "n_chunks_read": None,
            }
        except Exception as e:
            latency_ms = (time.perf_counter() - started) * 1000.0
            return {
                "status": "archive_error",
                "result": None,
                "latency_ms": latency_ms,
                "error": str(e),
                "n_chunks_read": None,
            }
        latency_ms = (time.perf_counter() - started) * 1000.0

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as e:
            return {
                "status": "archive_error",
                "result": None,
                "latency_ms": latency_ms,
                "error": f"json decode: {e}",
                "n_chunks_read": None,
            }

        # Prometheus error envelope (e.g., engine returned
        # `EngineError::CapabilityMiss` → 404 with `errorType:bad_data`).
        if parsed.get("status") == "error":
            err_type = parsed.get("errorType", "")
            archive_status = "archive_miss" if err_type == "bad_data" else "archive_error"
            return {
                "status": archive_status,
                "result": None,
                "latency_ms": latency_ms,
                "error": parsed.get("error"),
                "n_chunks_read": None,
            }

        # Verify the wire response actually came from the archive
        # engine. The backend annotates every response with a
        # `data_source: <id>` info-line — if it doesn't say
        # `gorilla_archive`, the override header didn't take effect
        # (e.g., the backend predates Fix 1). Treat as
        # `archive_error` so the operator notices.
        infos = parsed.get("infos") or []
        expected = f"data_source: {self.engine_id}"
        if infos and not any(s == expected for s in infos):
            actual = next(
                (s.split(": ", 1)[1] for s in infos if s.startswith("data_source: ")),
                "<missing>",
            )
            return {
                "status": "archive_error",
                "result": None,
                "latency_ms": latency_ms,
                "error": (
                    f"engine override ignored: expected data_source={self.engine_id}, "
                    f"got {actual} (backend likely predates Fix 1)"
                ),
                "n_chunks_read": None,
            }

        data = parsed.get("data") or {}
        result = data.get("result")
        # Empty instant-vector with no error → archive miss
        # (everything plumbed, but no chunk on S3 covers this query).
        if result == [] or result is None:
            return {
                "status": "archive_miss",
                "result": result,
                "latency_ms": latency_ms,
                "error": None,
                "n_chunks_read": None,
            }
        return {
            "status": "ok",
            "result": result,
            "latency_ms": latency_ms,
            "error": None,
            "n_chunks_read": None,
        }


# --- legacy JSONL ground-truth loader (transition-only) ------------


def iter_truth_samples(cold_truth_dir: str, metric: str) -> Iterable[dict]:
    """Yield {ts_ms, labels, value} from every part-*.jsonl under
    `<cold_truth_dir>/<metric>/...`. Tolerates a torn last line
    (the writer might still be flushing when the snapshot was
    taken). Used only when `--use-jsonl` is passed; the default
    archive-truth path bypasses this entirely."""
    pat = os.path.join(cold_truth_dir, metric, "*", "*", "*", "*", "part-*.jsonl")
    files = sorted(glob.glob(pat))
    if not files:
        return
    for path in files:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    # Torn last line tolerated; everything else is
                    # caller's problem.
                    continue


# --- query parsing -------------------------------------------------


# Quantile shape #1 (legacy): `histogram_quantile(0.5, sum by (le) (metric))`
_QUANTILE_HIST_RE = re.compile(
    r"histogram_quantile\(\s*([0-9.]+)\s*,\s*sum\s+by\s+\(\s*le\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
# Quantile shape #2 (post-#266 e2e queries): `quantile_over_time(0.5, metric_quantile[1m])`.
# We treat the "_quantile" suffix as a sketch-projection naming convention; the
# underlying ground truth is the raw metric without the suffix. Many of our
# replay queries are warm-tier `quantile_over_time(φ, *_quantile[1m])` which
# the engine routes to the DDSketch / KLL precompute output. The cold-truth
# metric directory is the unsuffixed name (raw_tee writes one dir per
# top-level metric). Handle both: reduce against the canonical
# unsuffixed metric.
_QUANTILE_OVER_TIME_RE = re.compile(
    r"quantile_over_time\(\s*([0-9.]+)\s*,\s*([\w_]+?)(?:_quantile)?\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)
_TOPK_RE = re.compile(r"topk\(\s*(\d+)\s*,\s*([\w_]+)\s*\)", re.IGNORECASE)
# count_unique shape #1 (legacy): `count(count by (X)(metric))` — distinct
# values of X across the series set.
_COUNT_UNIQUE_GROUP_RE = re.compile(
    r"count\(\s*count\s+by\s+\(\s*([\w_]+)\s*\)\s*\(\s*([\w_]+)\s*\)\s*\)",
    re.IGNORECASE,
)
# count_unique shape #2 (post-#266 e2e queries): `count(metric)` — distinct
# series count, equivalent to "how many time series exist for this metric".
# `truth_count_unique(samples, by="series")` re-uses the labels-set as the
# distinguishing key (computed in truth_count_distinct_series).
_COUNT_SERIES_RE = re.compile(r"^count\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
# sum shape #1 (legacy): `sum(metric)` — sum across all series.
_SUM_INSTANT_RE = re.compile(r"^sum\(\s*([\w_]+)\s*\)$", re.IGNORECASE)
# sum shape #2 (post-#266 e2e queries): `sum_over_time(metric[1m])` — sum
# across the trailing 1m window for each series. Truth maps to total sum
# across all samples in the cold-window since the replay queries an instant
# at end-of-soak — the cold-truth set covers the full soak.
_SUM_OVER_TIME_RE = re.compile(
    r"sum_over_time\(\s*([\w_]+)\s*\[\s*[0-9smhd]+\s*\]\s*\)",
    re.IGNORECASE,
)


def parse_query(promql: str) -> tuple[str, dict] | None:
    """Returns (kind, params). None if the shape isn't one we
    recognise; the row gets skipped with a logged warning."""
    s = promql.strip()
    if (m := _QUANTILE_HIST_RE.match(s)):
        return "quantile", {"q": float(m.group(1)), "metric": m.group(2)}
    if (m := _QUANTILE_OVER_TIME_RE.match(s)):
        return "quantile", {"q": float(m.group(1)), "metric": m.group(2)}
    if (m := _TOPK_RE.match(s)):
        return "topk", {"k": int(m.group(1)), "metric": m.group(2)}
    if (m := _COUNT_UNIQUE_GROUP_RE.match(s)):
        return "count_unique", {"by": m.group(1), "metric": m.group(2)}
    if (m := _COUNT_SERIES_RE.match(s)):
        return "count_unique", {"by": "__series__", "metric": m.group(1)}
    if (m := _SUM_INSTANT_RE.match(s)):
        return "sum", {"metric": m.group(1)}
    if (m := _SUM_OVER_TIME_RE.match(s)):
        return "sum", {"metric": m.group(1)}
    return None


# --- ground-truth computers (legacy JSONL path) --------------------


def truth_quantile(samples: list[dict], q: float) -> float:
    if not samples:
        return float("nan")
    vals = sorted(s["value"] for s in samples)
    if not vals:
        return float("nan")
    # Nearest-rank quantile. Matches what most sketches target,
    # within ε tolerance.
    n = len(vals)
    idx = max(0, min(n - 1, int(q * n)))
    return vals[idx]


def truth_topk(samples: list[dict], k: int) -> list[tuple[str, float]]:
    """Top-K by sum(value) with the full attribute set as the
    grouping key. Returns sorted descending."""
    bucket: Counter[str] = Counter()
    for s in samples:
        key = json.dumps(s.get("labels", {}), sort_keys=True)
        bucket[key] += s["value"]
    return bucket.most_common(k)


def truth_count_unique(samples: list[dict], by: str) -> int:
    if by == "__series__":
        # Distinct series count: full labels-set as key. Mirrors what
        # `count(metric)` returns in PromQL — number of distinct
        # time series for the metric.
        return len({json.dumps(s.get("labels", {}), sort_keys=True) for s in samples})
    return len({s.get("labels", {}).get(by) for s in samples})


def truth_sum(samples: list[dict]) -> float:
    return sum(s["value"] for s in samples)


# --- result extraction (PromQL → scalar / list) --------------------


def extract_scalar(result) -> float | None:
    if not result:
        return None
    if isinstance(result, list) and result:
        first = result[0]
        if isinstance(first, dict) and "value" in first:
            v = first["value"]
            if isinstance(v, list) and len(v) >= 2:
                try:
                    return float(v[1])
                except (TypeError, ValueError):
                    return None
    if isinstance(result, dict) and "value" in result:
        v = result["value"]
        if isinstance(v, list) and len(v) >= 2:
            try:
                return float(v[1])
            except (TypeError, ValueError):
                return None
    return None


def extract_topk_keys(result, k: int) -> list[str]:
    if not isinstance(result, list):
        return []
    keys: list[str] = []
    for el in result[:k]:
        if not isinstance(el, dict):
            continue
        m = el.get("metric") or {}
        keys.append(json.dumps(m, sort_keys=True))
    return keys


# --- per-cell reducer ----------------------------------------------


# CSV columns emitted by the Fix-1 reducer. `truth` and `error` are
# kept (renamed to `archive_answer` and `rel_err`) for backwards
# compatibility with downstream readers (`mvp_report.py`,
# `e2e_plots.py`) — both old and new names point at the same value.
FIELDS = [
    "cell",
    "kind",
    "query",
    "t",
    "duration_ms",
    "plan_id",
    # warm tier (under-test) answer, lifted from `replay.jsonl`
    "warm_answer",
    # archive (ground truth) answer, fetched via X-ASAP-Engine override
    "archive_answer",
    # relative error (or recall, for topk)
    "rel_err",
    "recall",
    # cost-of-truth-fetching, so the report can break out the latency
    # of cross-tier validation from the demo's primary query latency.
    "archive_query_latency_ms",
    "archive_status",  # ok | archive_miss | archive_error | parse_skip
    "n_chunks_read",
    # legacy aliases — mvp_report.py reads `truth`/`answer`/`error`/`n_truth_samples`
    "truth",
    "answer",
    "error",
    "n_truth_samples",
]


def _format_float(x: float, prec: int = 6) -> str:
    """Float→str that emits "" for NaN and a fixed-precision value
    otherwise. Keeps the CSV easy to grep without leaking `nan`."""
    if x != x or math.isinf(x):
        return ""
    return f"{x:.{prec}f}"


def reduce_cell_via_archive(
    cell_dir: str,
    writer: csv.DictWriter,
    cell_label: str,
    archive_client: ArchiveTruthClient,
) -> int:
    """Default Fix-1 path: ground truth comes from the Gorilla archive.

    For each replay row we re-issue the same PromQL against the
    backend with `X-ASAP-Engine: gorilla_archive`, parse the
    archive's answer, and compute relative error (or topk recall)
    against the warm-tier answer the replay client recorded.
    """
    replay_path = os.path.join(cell_dir, "replay.jsonl")
    if not os.path.exists(replay_path):
        print(f"[skip] no replay.jsonl in {cell_dir}", file=sys.stderr)
        return 0

    n_rows = 0
    n_archive_miss = 0
    n_archive_error = 0
    with open(replay_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            promql = rec.get("query", "")
            parsed = parse_query(promql)

            row = {
                "cell": cell_label,
                "kind": "",
                "query": promql,
                "t": rec.get("ts"),
                "duration_ms": rec.get("duration_ms"),
                "plan_id": rec.get("plan_id"),
                "warm_answer": "",
                "archive_answer": "",
                "rel_err": "",
                "recall": "",
                "archive_query_latency_ms": "",
                "archive_status": "",
                "n_chunks_read": "",
                # legacy aliases default to empty
                "truth": "",
                "answer": "",
                "error": "",
                "n_truth_samples": "",
            }

            if parsed is None:
                row["archive_status"] = "parse_skip"
                writer.writerow(row)
                n_rows += 1
                continue
            kind, params = parsed
            row["kind"] = kind

            # Per-row archive query — the cost-of-truth-fetching the
            # final report calls out separately from primary query
            # latency.
            archive = archive_client.query(promql)
            row["archive_query_latency_ms"] = _format_float(archive["latency_ms"], 3)
            row["archive_status"] = archive["status"]
            if archive.get("n_chunks_read") is not None:
                row["n_chunks_read"] = str(archive["n_chunks_read"])

            warm_result = rec.get("result")

            if archive["status"] != "ok":
                # Archive miss / error → no rel-err; row still
                # written so the report can count the misses. The
                # warm answer is preserved for diagnostic value.
                if archive["status"] == "archive_miss":
                    n_archive_miss += 1
                elif archive["status"] == "archive_error":
                    n_archive_error += 1
                # Capture the warm answer for the row so the report
                # can still see what the demo's replay client got.
                if kind == "topk":
                    sketch_keys = extract_topk_keys(warm_result, params["k"])
                    row["warm_answer"] = json.dumps(sketch_keys)[:120]
                    row["answer"] = row["warm_answer"]
                else:
                    a = extract_scalar(warm_result)
                    if a is not None:
                        row["warm_answer"] = _format_float(a)
                        row["answer"] = row["warm_answer"]
                writer.writerow(row)
                n_rows += 1
                continue

            archive_result = archive["result"]

            if kind == "quantile":
                t = extract_scalar(archive_result)
                a = extract_scalar(warm_result)
                if t is not None:
                    row["archive_answer"] = _format_float(t)
                    row["truth"] = row["archive_answer"]
                if a is not None:
                    row["warm_answer"] = _format_float(a)
                    row["answer"] = row["warm_answer"]
                if a is not None and t is not None:
                    rel = abs(a - t) / max(abs(t), 1.0)
                    row["rel_err"] = _format_float(rel)
                    row["error"] = row["rel_err"]
            elif kind == "topk":
                # Archive's exact top-K — same shape as the warm
                # answer (Prometheus instant vector).
                truth_keys = extract_topk_keys(archive_result, params["k"])
                sketch_keys = extract_topk_keys(warm_result, params["k"])
                if truth_keys:
                    overlap = len(set(truth_keys) & set(sketch_keys))
                    rec_v = overlap / len(truth_keys)
                    row["recall"] = f"{rec_v:.4f}"
                row["archive_answer"] = json.dumps(truth_keys)[:120]
                row["warm_answer"] = json.dumps(sketch_keys)[:120]
                row["truth"] = row["archive_answer"]
                row["answer"] = row["warm_answer"]
            elif kind == "count_unique":
                t = extract_scalar(archive_result)
                a = extract_scalar(warm_result)
                if t is not None:
                    row["archive_answer"] = f"{t:.0f}"
                    row["truth"] = row["archive_answer"]
                if a is not None:
                    row["warm_answer"] = f"{a:.0f}"
                    row["answer"] = row["warm_answer"]
                if a is not None and t is not None:
                    rel = abs(a - t) / max(t, 1)
                    row["rel_err"] = _format_float(rel)
                    row["error"] = row["rel_err"]
            elif kind == "sum":
                t = extract_scalar(archive_result)
                a = extract_scalar(warm_result)
                if t is not None:
                    row["archive_answer"] = _format_float(t)
                    row["truth"] = row["archive_answer"]
                if a is not None:
                    row["warm_answer"] = _format_float(a)
                    row["answer"] = row["warm_answer"]
                if a is not None and t is not None:
                    rel = abs(a - t) / max(abs(t), 1.0)
                    row["rel_err"] = _format_float(rel)
                    row["error"] = row["rel_err"]

            writer.writerow(row)
            n_rows += 1

    if n_archive_miss or n_archive_error:
        print(
            f"[{cell_label}] archive_miss={n_archive_miss} archive_error={n_archive_error}",
            file=sys.stderr,
        )
    return n_rows


def reduce_cell_via_jsonl(cell_dir: str, writer: csv.DictWriter, cell_label: str) -> int:
    """Legacy v1 path: ground truth comes from the per-cell
    `cold-truth/` JSONL stream produced by raw_tee. Retained behind
    `--use-jsonl` for the Fix-1 transition.
    """
    replay_path = os.path.join(cell_dir, "replay.jsonl")
    cold_root = os.path.join(cell_dir, "cold-truth")
    if not os.path.exists(replay_path):
        print(f"[skip] no replay.jsonl in {cell_dir}", file=sys.stderr)
        return 0
    if not os.path.isdir(cold_root):
        print(f"[skip] no cold-truth/ in {cell_dir}", file=sys.stderr)
        return 0

    # Group truth samples by metric name. We don't ts-bucket
    # because the replay queries are instant queries against the
    # whole cold window — match that scope.
    truth_by_metric: dict[str, list[dict]] = defaultdict(list)
    metric_dirs = [
        d for d in os.listdir(cold_root) if os.path.isdir(os.path.join(cold_root, d))
    ]
    for metric in metric_dirs:
        for s in iter_truth_samples(cold_root, metric):
            truth_by_metric[metric].append(s)

    n_rows = 0
    with open(replay_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            parsed = parse_query(rec["query"])
            if parsed is None:
                continue
            kind, params = parsed
            metric = params.get("metric", "")
            samples = truth_by_metric.get(metric, [])

            row = {
                "cell": cell_label,
                "kind": kind,
                "query": rec["query"],
                "t": rec["ts"],
                "duration_ms": rec.get("duration_ms"),
                "plan_id": rec.get("plan_id"),
                "warm_answer": "",
                "archive_answer": "",
                "rel_err": "",
                "recall": "",
                "archive_query_latency_ms": "",
                "archive_status": "jsonl",
                "n_chunks_read": "",
                # legacy aliases
                "n_truth_samples": len(samples),
                "truth": "",
                "answer": "",
                "error": "",
            }

            if kind == "quantile":
                t = truth_quantile(samples, params["q"])
                a = extract_scalar(rec.get("result"))
                row["truth"] = f"{t:.6f}" if t == t else ""  # nan check
                row["archive_answer"] = row["truth"]
                if a is not None and t == t:
                    row["answer"] = f"{a:.6f}"
                    row["warm_answer"] = row["answer"]
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"
                    row["rel_err"] = row["error"]
            elif kind == "topk":
                truth_pairs = truth_topk(samples, params["k"])
                truth_keys = [k for k, _ in truth_pairs]
                sketch_keys = extract_topk_keys(rec.get("result"), params["k"])
                if truth_keys:
                    overlap = len(set(truth_keys) & set(sketch_keys))
                    row["recall"] = f"{overlap / len(truth_keys):.4f}"
                row["truth"] = json.dumps(truth_keys)[:120]
                row["answer"] = json.dumps(sketch_keys)[:120]
                row["archive_answer"] = row["truth"]
                row["warm_answer"] = row["answer"]
            elif kind == "count_unique":
                t = truth_count_unique(samples, params["by"])
                a = extract_scalar(rec.get("result"))
                row["truth"] = str(t)
                row["archive_answer"] = row["truth"]
                if a is not None:
                    row["answer"] = f"{a:.0f}"
                    row["warm_answer"] = row["answer"]
                    row["error"] = f"{abs(a - t) / max(t, 1):.6f}"
                    row["rel_err"] = row["error"]
            elif kind == "sum":
                t = truth_sum(samples)
                a = extract_scalar(rec.get("result"))
                row["truth"] = f"{t:.6f}"
                row["archive_answer"] = row["truth"]
                if a is not None:
                    row["answer"] = f"{a:.6f}"
                    row["warm_answer"] = row["answer"]
                    row["error"] = f"{abs(a - t) / max(abs(t), 1.0):.6f}"
                    row["rel_err"] = row["error"]

            writer.writerow(row)
            n_rows += 1
    return n_rows


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Accuracy reducer (P8)")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--cell-dir", help="single cell directory to reduce")
    g.add_argument("--sweep-root", help="sweep root containing many cell dirs")
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--backend",
        default=os.environ.get("ASAP_BACKEND_URL", DEFAULT_BACKEND_URL),
        help=(
            "backend HTTP base URL — used to fetch ground truth via the "
            "Gorilla archive engine (Phase-6 Fix 1). Honours "
            "$ASAP_BACKEND_URL when --backend is unset."
        ),
    )
    ap.add_argument(
        "--archive-engine-id",
        default=DEFAULT_ARCHIVE_ENGINE_ID,
        help=(
            "data_source_id of the engine to query for ground truth "
            f"(default: {DEFAULT_ARCHIVE_ENGINE_ID})"
        ),
    )
    ap.add_argument(
        "--archive-timeout",
        type=float,
        default=DEFAULT_ARCHIVE_TIMEOUT_S,
        help=f"per-archive-query timeout in seconds (default: {DEFAULT_ARCHIVE_TIMEOUT_S})",
    )
    ap.add_argument(
        "--use-jsonl",
        action="store_true",
        help=(
            "[transition-only] fall back to the legacy JSONL "
            "ground-truth tee. Default behaviour queries the backend's "
            "Gorilla archive engine for apples-to-apples ground truth."
        ),
    )
    args = ap.parse_args(argv)

    cells: list[tuple[str, str]] = []
    if args.cell_dir:
        cells.append((args.cell_dir, os.path.basename(args.cell_dir.rstrip("/"))))
    else:
        for entry in sorted(os.listdir(args.sweep_root)):
            full = os.path.join(args.sweep_root, entry)
            if os.path.isdir(full) and os.path.exists(os.path.join(full, "replay.jsonl")):
                cells.append((full, entry))

    archive_client: ArchiveTruthClient | None = None
    if not args.use_jsonl:
        archive_client = ArchiveTruthClient(
            backend_url=args.backend,
            engine_id=args.archive_engine_id,
            timeout_s=args.archive_timeout,
        )
        print(
            f"reduce: ground truth via {args.backend} "
            f"(X-ASAP-Engine: {args.archive_engine_id})",
            file=sys.stderr,
        )
    else:
        print("reduce: ground truth via legacy JSONL tee (--use-jsonl)", file=sys.stderr)

    total = 0
    with open(args.out, "w", newline="") as fout:
        w = csv.DictWriter(fout, fieldnames=FIELDS)
        w.writeheader()
        for cell_dir, label in cells:
            if archive_client is not None:
                rows = reduce_cell_via_archive(cell_dir, w, label, archive_client)
            else:
                rows = reduce_cell_via_jsonl(cell_dir, w, label)
            print(f"[{label}] {rows} rows")
            total += rows

    print(f"reduce: total {total} rows → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
