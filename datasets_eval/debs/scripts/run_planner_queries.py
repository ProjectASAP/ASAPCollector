"""Sends /api/v1/plan for each DEBS 2022 query and saves results to canonical_results.json."""

import json
import subprocess
import sys
import time
import requests
import os
import signal

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, "../../.."))
CONTROLLER_DIR = os.path.join(REPO_ROOT, "controller")
CONTROLLER_BIN = os.path.join(CONTROLLER_DIR, "target/release/controller")
CONTROLLER_URL = "http://127.0.0.1:8080"
PLAN_ENDPOINT = f"{CONTROLLER_URL}/api/v1/plan"

DEFAULT_WORKLOAD = {
    "series_count": 5178,
    "samples_per_sec_per_series": 0.2,
    "bytes_per_raw_sample": 100,
    "data_distribution": "zipf",
}
DEFAULT_ACCURACY_SLA = 0.01

CANONICAL_QUERIES = [

    # Q1
    {
        "id": "Q1-sql",
        "label": "Q1 EMA recursive CTE (SQL)",
        "query": """WITH ordered AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
ema AS (
  SELECT symbol, last, ts, rn,
         last AS ema38,
         last AS ema100
  FROM   ordered WHERE rn = 1

  UNION ALL

  SELECT o.symbol, o.last, o.ts, o.rn,
         (2.0/39)  * o.last + (1 - 2.0/39)  * e.ema38,
         (2.0/101) * o.last + (1 - 2.0/101) * e.ema100
  FROM   ordered o
  JOIN   ema e ON e.symbol = o.symbol AND e.rn = o.rn - 1
)
SELECT symbol, ts, ema38, ema100
FROM   ema
ORDER  BY symbol, ts""",
    },

    # Q2
    {
        "id": "Q2-sql",
        "label": "Q2 EMA crossover CASE+LAG (SQL)",
        "query": """WITH q1 AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn,
         last AS ema38, last AS ema100
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
diffs AS (
  SELECT symbol, ts, ema38 - ema100 AS diff,
         LAG(ema38 - ema100) OVER (PARTITION BY symbol ORDER BY ts) AS prev_diff
  FROM   q1
)
SELECT symbol, ts,
       CASE
         WHEN prev_diff <= 0 AND diff > 0 THEN 'bullish'
         WHEN prev_diff >= 0 AND diff < 0 THEN 'bearish'
       END AS signal
FROM   diffs
WHERE  (prev_diff <= 0 AND diff > 0)
    OR (prev_diff >= 0 AND diff < 0)""",
    },

    # Q3
    {
        "id": "Q3-promql",
        "label": "Q3 count per symbol frequency (PromQL)",
        "query": "count_over_time(financial_last_trade_price[5m])",
    },
    {
        "id": "Q3-sql",
        "label": "Q3 count per symbol frequency (SQL)",
        "query": """SELECT symbol, COUNT(*) AS freq
FROM   financial_last_trade_price
GROUP  BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)""",
    },

    # Q4
    {
        "id": "Q4-promql-max",
        "label": "Q4 max_over_time (PromQL)",
        "query": 'max_over_time(financial_last_trade_price{sectype="E"}[5m])',
    },
    {
        "id": "Q4-promql-min",
        "label": "Q4 min_over_time (PromQL)",
        "query": 'min_over_time(financial_last_trade_price{sectype="E"}[5m])',
    },
    {
        "id": "Q4-promql-last",
        "label": "Q4 last_over_time (PromQL)",
        "query": 'last_over_time(financial_last_trade_price{sectype="E"}[5m])',
    },
    {
        "id": "Q4-sql",
        "label": "Q4 MAX/MIN/LAST_VALUE (SQL)",
        "query": """SELECT symbol,
       MAX(last) AS high,
       MIN(last) AS low,
       LAST_VALUE(last) OVER (
         PARTITION BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)
         ORDER BY ts
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS last_price,
       MAX(last) - MIN(last) AS range
FROM   financial_last_trade_price
WHERE  sectype = 'E'
GROUP  BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)""",
    },

    # Q5
    {
        "id": "Q5-sql",
        "label": "Q5 realized vol STDDEV_SAMP (SQL)",
        "query": """WITH ticks AS (
  SELECT symbol, ts, last,
         LN(last / LAG(last) OVER (PARTITION BY symbol ORDER BY ts)) AS log_return
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
windowed AS (
  SELECT symbol, log_return,
         TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start
  FROM   ticks
  WHERE  log_return IS NOT NULL
)
SELECT symbol, window_start,
       STDDEV_SAMP(log_return) AS realized_vol
FROM   windowed
GROUP  BY symbol, window_start""",
    },

    # Q6
    {
        "id": "Q6-promql",
        "label": "Q6 distinct symbol count (PromQL)",
        "query": "count(count_over_time(financial_last_trade_price[5m]))",
    },
    {
        "id": "Q6-sql",
        "label": "Q6 COUNT(DISTINCT symbol) (SQL)",
        "query": """SELECT TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
       COUNT(DISTINCT symbol) AS active_symbols
FROM   financial_last_trade_price
GROUP  BY TUMBLE_START(ts, INTERVAL '5' MINUTE)""",
    },

    # Q7
    {
        "id": "Q7-promql",
        "label": "Q7 TWAP avg_over_time (PromQL)",
        "query": 'avg_over_time(financial_last_trade_price{sectype="E"}[5m])',
    },
    {
        "id": "Q7-sql",
        "label": "Q7 TWAP AVG (SQL)",
        "query": """SELECT symbol,
       TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
       AVG(last) AS twap
FROM   financial_last_trade_price
WHERE  sectype = 'E'
GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '5' MINUTE)""",
    },

    # Q8
    {
        "id": "Q8-sql-zscore",
        "label": "Q8 anomaly z-score AVG+STDDEV_SAMP (SQL)",
        "query": """WITH stats AS (
  SELECT symbol, ts, last,
         TUMBLE_START(ts, INTERVAL '15' MINUTE) AS window_start,
         AVG(last) OVER w AS mu,
         STDDEV_SAMP(last) OVER w AS sigma
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  WINDOW w AS (PARTITION BY symbol, TUMBLE(ts, INTERVAL '15' MINUTE))
)
SELECT symbol, ts, last,
       (last - mu) / NULLIF(sigma, 0) AS z_score,
       CASE WHEN ABS((last - mu) / NULLIF(sigma, 0)) > 2.5
            THEN true ELSE false END AS is_anomaly
FROM   stats""",
    },
    {
        "id": "Q8-sql-iqr",
        "label": "Q8 anomaly IQR PERCENTILE_CONT (SQL)",
        "query": """WITH quartiles AS (
  SELECT symbol,
         TUMBLE_START(ts, INTERVAL '15' MINUTE) AS window_start,
         PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY last) AS q1,
         PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY last) AS q3
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '15' MINUTE)
)
SELECT f.symbol, f.ts, f.last,
       CASE WHEN f.last < q.q1 - 1.5*(q.q3 - q.q1)
              OR f.last > q.q3 + 1.5*(q.q3 - q.q1)
            THEN true ELSE false
       END AS is_anomaly
FROM   financial_last_trade_price f
JOIN   quartiles q ON f.symbol = q.symbol
       AND TUMBLE_START(f.ts, INTERVAL '15' MINUTE) = q.window_start""",
    },

    # Q9
    {
        "id": "Q9-sql",
        "label": "Q9 Bollinger bands AVG+STDDEV_SAMP (SQL)",
        "query": """WITH bars AS (
  SELECT symbol,
         TUMBLE_START(ts, INTERVAL '5' MINUTE) AS bar_start,
         AVG(last) AS bar_avg
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  GROUP  BY symbol, TUMBLE_START(ts, INTERVAL '5' MINUTE)
),
bands AS (
  SELECT symbol, bar_start,
         AVG(bar_avg)         OVER w AS sma,
         STDDEV_SAMP(bar_avg) OVER w AS sigma
  FROM   bars
  WINDOW w AS (PARTITION BY symbol ORDER BY bar_start ROWS 2 PRECEDING)
)
SELECT symbol, bar_start,
       sma,
       sma + 2 * sigma AS upper_band,
       sma - 2 * sigma AS lower_band
FROM   bands
WHERE  sigma IS NOT NULL""",
    },

    # Q10
    {
        "id": "Q10-sql",
        "label": "Q10 RSI(14) Wilder's EMA (SQL)",
        "query": """WITH changes AS (
  SELECT symbol, ts, last,
         last - LAG(last) OVER (PARTITION BY symbol ORDER BY ts) AS delta
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
gains_losses AS (
  SELECT symbol, ts,
         GREATEST(delta, 0)  AS gain,
         GREATEST(-delta, 0) AS loss,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   changes
  WHERE  delta IS NOT NULL
),
seed AS (
  SELECT symbol,
         AVG(gain) AS avg_gain,
         AVG(loss) AS avg_loss,
         MAX(ts)   AS ts,
         14        AS rn
  FROM   gains_losses
  WHERE  rn <= 14
  GROUP  BY symbol
),
rsi_calc AS (
  SELECT symbol, ts, avg_gain, avg_loss, rn FROM seed
  UNION ALL
  SELECT g.symbol, g.ts,
         (r.avg_gain * 13 + g.gain) / 14.0,
         (r.avg_loss * 13 + g.loss) / 14.0,
         g.rn
  FROM   gains_losses g
  JOIN   rsi_calc r ON r.symbol = g.symbol AND g.rn = r.rn + 1
)
SELECT symbol, ts,
       100 - 100 / (1 + avg_gain / NULLIF(avg_loss, 0)) AS rsi
FROM   rsi_calc
WHERE  rn > 14
ORDER  BY symbol, ts""",
    },

    # Q11
    {
        "id": "Q11-sql",
        "label": "Q11 MACD(12/26/9) recursive CTE (SQL)",
        "query": """WITH ordered AS (
  SELECT symbol, last, ts,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts) AS rn
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
),
macd AS (
  SELECT symbol, ts, rn, last,
         last AS ema12,
         last AS ema26,
         0.0  AS macd_line,
         0.0  AS signal_line
  FROM   ordered WHERE rn = 1

  UNION ALL

  SELECT o.symbol, o.ts, o.rn, o.last,
         (2.0/13) * o.last + (1 - 2.0/13) * m.ema12,
         (2.0/27) * o.last + (1 - 2.0/27) * m.ema26,
         ((2.0/13) * o.last + (1 - 2.0/13) * m.ema12)
           - ((2.0/27) * o.last + (1 - 2.0/27) * m.ema26),
         (2.0/10) * (
           ((2.0/13) * o.last + (1 - 2.0/13) * m.ema12)
           - ((2.0/27) * o.last + (1 - 2.0/27) * m.ema26)
         ) + (1 - 2.0/10) * m.signal_line
  FROM   ordered o
  JOIN   macd m ON m.symbol = o.symbol AND m.rn = o.rn - 1
)
SELECT symbol, ts, ema12, ema26,
       macd_line,
       signal_line,
       macd_line - signal_line AS histogram
FROM   macd
ORDER  BY symbol, ts""",
    },

    # Q12
    {
        "id": "Q12-sql",
        "label": "Q12 stochastic %K/%D rolling MIN/MAX (SQL)",
        "query": """WITH rolling AS (
  SELECT symbol, ts, last,
         MIN(last) OVER w AS low_14,
         MAX(last) OVER w AS high_14
  FROM   financial_last_trade_price
  WHERE  sectype = 'E'
  WINDOW w AS (PARTITION BY symbol ORDER BY ts ROWS 13 PRECEDING)
),
pct_k AS (
  SELECT symbol, ts, last,
         100.0 * (last - low_14) / NULLIF(high_14 - low_14, 0) AS k
  FROM   rolling
)
SELECT symbol, ts, k,
       AVG(k) OVER (PARTITION BY symbol ORDER BY ts ROWS 2 PRECEDING) AS d
FROM   pct_k
ORDER  BY symbol, ts""",
    },

    # Q13
    {
        "id": "Q13-promql",
        "label": "Q13 top-K symbols by median price topk(avg_over_time) (PromQL)",
        "query": 'topk(10, avg_over_time(financial_last_trade_price{sectype="E"}[5m]))',
    },

]




def start_controller():
    env = os.environ.copy()
    env["CONTROLLER_ADDR"] = "127.0.0.1:8080"
    env["CONTROLLER_OPAMP_ADDR"] = "127.0.0.1:4320"
    env["CONTROLLER_OPAMP_ENDPOINT"] = "ws://127.0.0.1:4320/v1/opamp"
    env["CONTROLLER_SKETCH_DEFAULTS"] = os.path.join(
        CONTROLLER_DIR, "sketch_params_default.yml"
    )
    env["CONTROLLER_SKETCH_CAPABILITIES"] = os.path.join(
        CONTROLLER_DIR, "sketch_capabilities.yml"
    )
    env["RUST_LOG"] = "info"

    proc = subprocess.Popen(
        [CONTROLLER_BIN],
        cwd=CONTROLLER_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    # Wait for the server to come up
    for _ in range(30):
        try:
            requests.get(f"{CONTROLLER_URL}/api/v1/agents", timeout=1)
            return proc
        except Exception:
            time.sleep(0.5)
    proc.kill()
    out, err = proc.communicate()
    print("STDOUT:", out.decode(errors="replace"))
    print("STDERR:", err.decode(errors="replace"))
    raise RuntimeError("Controller failed to start within 15 seconds")


def send_query(q, accuracy_sla=DEFAULT_ACCURACY_SLA, workload=None):
    # Mangle metric name so each query gets its own planner slot (avoids caching)
    qid = q["id"].replace("-", "_")
    mangled_metric = f"financial_last_trade_price_{qid}"
    query_text = q["query"].replace("financial_last_trade_price", mangled_metric)

    body = {
        "query_string": query_text,
        "accuracy_sla": accuracy_sla,
        "workload": workload or DEFAULT_WORKLOAD,
    }
    try:
        r = requests.post(PLAN_ENDPOINT, json=body, timeout=10)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text
    except Exception as e:
        return 0, str(e)


def main():
    print("=" * 80)
    print("Starting controller (unmodified build)...")
    proc = start_controller()
    print(f"Controller started (PID {proc.pid})")
    print("=" * 80)

    all_results = {}

    for q in CANONICAL_QUERIES:
        qid = q["id"]
        label = q["label"]
        query_text = q["query"].strip()
        lang = "PromQL" if "promql" in qid else "SQL"

        print(f"\n{'─' * 70}")
        print(f"  [{qid}] {label}")
        print(f"  Language: {lang}")
        print(f"  Query:    {query_text[:100]}{'…' if len(query_text) > 100 else ''}")

        status, body = send_query(q)
        all_results[qid] = {"label": label, "lang": lang, "status": status, "response": body}

        if status == 200 and isinstance(body, dict):
            print(f"  ✓ Status: {status}")
            print(f"    metric:       {body.get('metric', '?')}")
            print(f"    sketch_type:  {body.get('sketch_type', '?')}")
            print(f"    mode:         {body.get('mode', '?')}")
            print(f"    aggregate_by: {body.get('aggregate_by', '?')}")
            tc = body.get("transmission_costs", {})
            if tc:
                print(f"    raw B/s:      {tc.get('raw_bytes_per_sec', '?'):.1f}")
                print(f"    sketch B/s:   {tc.get('sketch_full_bytes_per_sec', '?'):.1f}")
                print(f"    delta B/s:    {tc.get('sketch_delta_bytes_per_sec', '?'):.1f}")
            dd = body.get("delta_decision")
            if dd:
                print(f"    delta_mode:   {dd.get('mode', '?')}")
            sp = body.get("staged_plan")
            if sp:
                print(f"    staged_plan:  {json.dumps(sp, indent=6)[:300]}")
            ps = body.get("plan_summary")
            if ps:
                print(f"    plan_summary: {json.dumps(ps, indent=6)[:500]}")
        else:
            print(f"  ✗ Status: {status}")
            if isinstance(body, dict):
                print(f"    Error: {json.dumps(body)[:300]}")
            else:
                print(f"    Error: {str(body)[:300]}")

    # Write full output to JSON
    out_file = os.path.join(SCRIPT_DIR, "canonical_results.json")
    with open(out_file, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n{'═' * 80}")
    print(f"Full results saved to {out_file}")

    # Summary table
    print(f"\n{'═' * 80}")
    print(f"  SUMMARY")
    print(f"{'═' * 80}")
    print(f"{'ID':<22} {'Lang':>6}  {'Status':>6}  {'Sketch':>15}  Label")
    print(f"{'─'*22} {'─'*6}  {'─'*6}  {'─'*15}  {'─'*40}")
    for qid, res in all_results.items():
        st = res["status"]
        lang = res.get("lang", "?")
        sketch = "—"
        if st == 200 and isinstance(res["response"], dict):
            sketch = res["response"].get("sketch_type", "—")
        print(f"{qid:<22} {lang:>6}  {st:>6}  {sketch:>15}  {res['label']}")

    # Shutdown
    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=5)
    print(f"\nController stopped.")


if __name__ == "__main__":
    main()
