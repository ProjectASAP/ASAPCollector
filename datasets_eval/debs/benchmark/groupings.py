"""Cross-key roll-up groupings for the DEBS benchmark harness.

This module defines functions that map a per-symbol sketch/ground-truth row
to a coarser group key (e.g., sector, "ALL", random bucket). The harness uses
these groupings in the ``crosskey`` mode of ``run.py`` to evaluate how sketch
error scales as we roll up across keys (per-symbol -> per-sector -> all-symbols).

A "row" is a dict-like (or pandas Series) with at least a ``symbol`` field.
Group functions are pure: they take a row and return a string key.

Two roll-up evaluation modes are supported by ``run.py crosskey``:

* ``--mode sketch_merge`` (preferred, requires ``transmit_sketch=true``):
  per-symbol sketch *byte payloads* are deserialized, merged with the sketch
  library's native merge operator, and the resulting group-level sketch is
  queried for the same metric used per-symbol. This is the analytically
  correct way to evaluate cross-key error.

* ``--mode point_rollup`` (fallback, always works):
  the per-symbol *scalar* sketch outputs are aggregated by a simple statistic
  (weighted average for means/quantiles, max for cardinality), and that
  scalar is compared against ground truth aggregated the same way. This is
  *not* equivalent to a true sketch merge -- it is a point estimate of how
  the per-symbol approximation propagates under naive aggregation -- but it
  is useful when sketch payloads are not published as Prometheus labels.

The chosen mode is recorded in the resulting CSV.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Mapping


# ---------------------------------------------------------------------------
# Sector taxonomy
# ---------------------------------------------------------------------------
# DEBS 2022 ships no sector mapping in either the dataset CSVs or the
# 01_dataset_statistics.md file; only ``sectype`` (E/I) is present, which is
# instrument-class, not sector. We therefore hardcode a small static map for
# the most active symbols on European exchanges (suffixes ``.NL``, ``.DE``,
# ``.F``, etc. appear in the corpus). Symbols not in this table fall back to
# ``OTHER``. This is intentionally coarse -- the table only needs to cover
# enough symbols to demonstrate fan-in scaling for the paper's plot.
#
# Symbols are matched on the *stem* (the part before the last ``.``). This
# matches how ``ground_truth.common.split_symbol_exchange`` strips the
# exchange suffix.
SYMBOL_TO_SECTOR: Mapping[str, str] = {
    # --- Energy
    "RDSA": "ENERGY",
    "RDSB": "ENERGY",
    "BP": "ENERGY",
    "TTE": "ENERGY",
    "ENI": "ENERGY",
    "REP": "ENERGY",
    # --- Financials
    "INGA": "FINANCIALS",
    "ABN": "FINANCIALS",
    "BNP": "FINANCIALS",
    "DBK": "FINANCIALS",
    "CBK": "FINANCIALS",
    "SAN": "FINANCIALS",
    "BBVA": "FINANCIALS",
    "ISP": "FINANCIALS",
    "UCG": "FINANCIALS",
    # --- Technology
    "ASML": "TECH",
    "SAP": "TECH",
    "ADYEN": "TECH",
    "STM": "TECH",
    "INFN": "TECH",
    "PRX": "TECH",
    # --- Consumer / Staples
    "AD": "CONSUMER",
    "HEIA": "CONSUMER",
    "ABI": "CONSUMER",
    "UNA": "CONSUMER",
    "DSM": "CONSUMER",
    "PHIA": "CONSUMER",
    # --- Industrials
    "AIR": "INDUSTRIALS",
    "MT": "INDUSTRIALS",
    "BAS": "INDUSTRIALS",
    "SIE": "INDUSTRIALS",
    "VOW3": "INDUSTRIALS",
    "BMW": "INDUSTRIALS",
    "DAI": "INDUSTRIALS",
    # --- Healthcare
    "SAN.PA": "HEALTHCARE",  # exact-key fallback example; see lookup below
}


def _row_value(row: Any, key: str) -> Any:
    """Read ``key`` from row, supporting dict, pandas Series, or attribute access."""
    if row is None:
        return None
    if isinstance(row, Mapping):
        return row.get(key)
    # pandas Series / namedtuple-like
    if hasattr(row, "get"):
        try:
            return row.get(key)
        except Exception:  # pragma: no cover - defensive
            pass
    return getattr(row, key, None)


def _symbol_stem(symbol: str) -> str:
    """Strip exchange suffix (``RDSA.NL`` -> ``RDSA``)."""
    if not symbol:
        return ""
    s = str(symbol)
    if "." in s:
        # ``rsplit`` mirrors ground_truth.common.split_symbol_exchange
        return s.rsplit(".", 1)[0]
    return s


# ---------------------------------------------------------------------------
# Public grouping functions
# ---------------------------------------------------------------------------
def per_symbol(row: Any) -> str:
    """Identity grouping: each symbol is its own group."""
    return str(_row_value(row, "symbol") or "")


def per_sector(row: Any) -> str:
    """Map symbol -> sector via :data:`SYMBOL_TO_SECTOR`; fall back to ``OTHER``."""
    sym = str(_row_value(row, "symbol") or "")
    if not sym:
        return "OTHER"
    # Exact match first (allows entries like ``SAN.PA`` to override stem-only).
    if sym in SYMBOL_TO_SECTOR:
        return SYMBOL_TO_SECTOR[sym]
    stem = _symbol_stem(sym)
    return SYMBOL_TO_SECTOR.get(stem, "OTHER")


def all_symbols(row: Any) -> str:
    """Single-bucket roll-up: every row collapses to ``ALL``."""
    return "ALL"


def random_buckets_n(row: Any, n: int) -> str:
    """Hash-bucket symbol into one of ``n`` buckets. Stable across Python runs."""
    if n <= 0:
        raise ValueError(f"random_buckets_n requires n > 0, got {n}")
    sym = str(_row_value(row, "symbol") or "")
    digest = hashlib.md5(sym.encode("utf-8"), usedforsecurity=False).digest()
    # Use first 8 bytes as an unsigned int.
    bucket = int.from_bytes(digest[:8], byteorder="big", signed=False) % n
    return f"BUCKET_{bucket:03d}"


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------
def resolve_grouping(name: str) -> Callable[[Any], str]:
    """Return the grouping function for ``name``.

    Accepts: ``per_symbol``, ``per_sector``, ``all`` / ``all_symbols``,
    ``random_<N>`` (e.g., ``random_8``, ``random_32``).
    """
    n = name.strip().lower()
    if n in ("per_symbol", "symbol"):
        return per_symbol
    if n in ("per_sector", "sector"):
        return per_sector
    if n in ("all", "all_symbols"):
        return all_symbols
    if n.startswith("random_"):
        try:
            k = int(n.split("_", 1)[1])
        except (IndexError, ValueError) as exc:
            raise ValueError(f"Bad random grouping: {name!r}") from exc
        return lambda row, _k=k: random_buckets_n(row, _k)
    raise ValueError(f"Unknown grouping: {name!r}")


def parse_grouping_list(spec: str) -> list[tuple[str, Callable[[Any], str]]]:
    """Parse a comma-separated grouping spec like ``per_symbol,per_sector,all,random_8``.

    Returns ``[(label, fn), ...]`` preserving order and dropping empty entries.
    """
    out: list[tuple[str, Callable[[Any], str]]] = []
    for raw in spec.split(","):
        name = raw.strip()
        if not name:
            continue
        out.append((name, resolve_grouping(name)))
    return out
