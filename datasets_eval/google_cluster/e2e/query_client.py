#!/usr/bin/env python3
"""Thin client for the ASAP data-plane query engine (asap_query).

Issues an instant MetricsQL/PromQL query against the data plane's
`/api/v1/query` surface (default node2 `:9091`) and returns the parsed
Prometheus-style response plus the `data_source` the engine reports —
so the harness can flag any query that fell through to the archive
tier instead of being answered warm.

Stdlib-only (urllib) so it runs without the requests dependency.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class QueryResult:
    query: str
    ok: bool
    result_type: str = ""           # "vector" | "scalar" | "matrix"
    series: list[dict[str, Any]] = field(default_factory=list)  # [{labels, value}]
    data_source: str = "unknown"    # "warm"/"sketch"/"precompute"/"thanos_archive"/...
    fallback_used: bool = False
    raw: dict[str, Any] = field(default_factory=dict)
    error: str = ""


def query_instant(
    base_url: str,
    promql: str,
    timeout_s: float = 20.0,
    engine_header: str | None = None,
) -> QueryResult:
    """Run an instant query. `engine_header` sets X-ASAP-Engine (e.g.
    'thanos_archive') for the optional archive cross-check."""
    url = base_url.rstrip("/") + "/api/v1/query?" + urllib.parse.urlencode({"query": promql})
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    if engine_header:
        req.add_header("X-ASAP-Engine", engine_header)
    hdr_src = None
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            hdr_src = resp.headers.get("X-ASAP-Data-Source") or resp.headers.get("X-ASAP-Engine")
    except urllib.error.HTTPError as exc:
        # The data plane returns 4xx with a JSON body for some error shapes;
        # parse it so callers still see data_source / error text.
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except Exception:  # noqa: BLE001
            return QueryResult(query=promql, ok=False, error=str(exc))
    except Exception as exc:  # noqa: BLE001 — transport failure
        return QueryResult(query=promql, ok=False, error=str(exc))

    if not isinstance(body, dict):
        return QueryResult(query=promql, ok=False, error="non-object response body")

    # data_source is reported in the `infos` array as "data_source: <X>"
    # (e.g. "asap_query", "thanos_archive"); fall back to header / body field.
    src = hdr_src or "unknown"
    for info in body.get("infos") or []:
        if isinstance(info, str) and info.strip().startswith("data_source:"):
            src = info.split(":", 1)[1].strip()
            break
    if body.get("data_source"):
        src = body["data_source"]

    # `data` is null on a "No result" response — that's a valid EMPTY result
    # (the warm tier answered, just no series), NOT a transport error.
    data = body.get("data") or {}
    rtype = data.get("resultType", "") if isinstance(data, dict) else ""
    series: list[dict[str, Any]] = []
    if isinstance(data, dict) and rtype in ("vector", "matrix"):
        for s in data.get("result", []):
            val = s.get("value") or (s.get("values") or [[None, None]])[-1]
            series.append({"labels": s.get("metric", {}),
                           "value": float(val[1]) if val and val[1] is not None else None})
    elif isinstance(data, dict) and rtype == "scalar":
        v = data.get("result")
        if v:
            series.append({"labels": {}, "value": float(v[1])})

    err = "" if body.get("status") != "error" else str(body.get("error", ""))
    return QueryResult(query=promql, ok=True, result_type=rtype, series=series,
                       data_source=src, fallback_used=bool(body.get("fallback_used")),
                       raw=body, error=err)


def await_ready(base_url: str, attempts: int = 30, delay_s: float = 2.0) -> bool:
    """Poll until the data plane answers a trivial query (or give up)."""
    import time
    for _ in range(attempts):
        r = query_instant(base_url, "vector(1)", timeout_s=5.0)
        if r.ok:
            return True
        time.sleep(delay_s)
    return False
