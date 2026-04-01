from __future__ import annotations

from pathlib import Path

DEBS_ROOT = Path(__file__).resolve().parent.parent
DEBS_TZ = "Europe/Berlin"
METRIC_NAME = "financial.last_trade_price"
DEFAULT_DAYS = ("08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21")

STATISTICAL_QUERIES = ("Q1", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8")
NOP_QUERIES = ("Q2", "Q9", "Q10", "Q11", "Q12")
FULL_FEED_QUERIES = ("Q3", "Q6")


def data_path(dataset: str) -> Path:
    if dataset == "data":
        return DEBS_ROOT / "data"
    if dataset == "data_filtered":
        return DEBS_ROOT / "data_filtered"
    raise ValueError(dataset)


def day_to_filename(day: str) -> str:
    stripped = day.strip()
    if stripped.endswith(".csv"):
        return stripped
    return f"debs2022-gc-trading-day-{stripped}.csv"


def split_symbol_exchange(symbol_id: str) -> tuple[str, str]:
    text = str(symbol_id).strip()
    if "." in text:
        symbol, exchange = text.rsplit(".", 1)
        return symbol, exchange
    return text, ""
