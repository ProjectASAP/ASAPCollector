## Accuracy thresholds

| Query | Metric                                        | Threshold |
| ----- | --------------------------------------------- | --------- |
| Q1    | `cms_s3_frequency_rel_error`                  | ≤ 0.05    |
| Q2    | `kll_duration_p99_rel_error`                  | ≤ 0.05    |
| Q3    | `dds_s3bytes_p99_rel_error`                   | ≤ 0.05    |
| Q4    | `cms_archetype_share_error`                   | ≤ 0.05    |
| Q5    | `hll_warehouse_cardinality_rel_error`         | ≤ 0.05    |

---

## Accuracy (sketch vs ground truth)

| query | slice | metric                           | value              | threshold | pass  |
| ----- | ----- | -------------------------------- | ------------------ | --------- | ----- |
| Q1    | full  | cms_freq_rel_err                 | 0.0405306963840109 | 0.05      | True  |
| Q2    | full  | kll_p99_rel_err                  | 0.2663046310935637 | 0.05      | False |
| Q3    | full  | dds_p99_rel_err                  | 0.0305166023669875 | 0.05      | True  |
| Q4    | full  | cms_arch_share_err               | 0.1984137055837563 | 0.05      | False |
| Q5    | full  | hll_cardinality_rel_err          | 0.3152173913043478 | 0.05      | False |

---

## Throughput

| query | slice | replay_mode | speed_factor | total_events | elapsed_s   | events_per_sec | export_count |
| ----- | ----- | ----------- | ------------ | ------------ | ----------- | -------------- | ------------ |
| Q1    | full  | scaled      | 1000         | 5004557      | 535.270087  | 9349.59215     | 1001         |
| Q3    | full  | scaled      | 1000         | 29930247     | 1270.546057 | 23556.994921   | 5987         |
| Q2    | full  | scaled      | 1000         | 69182074     | 2313.644545 | 29901.773005   | 13837        |
| Q4    | full  | scaled      | 1000         | 46718176     | 2309.788642 | 20226.169246   | 9344         |
| Q5    | full  | scaled      | 1000         | 69182074     | 1562.635706 | 44272.682198   | 13837        |

---

## Latency history

| query | slice | replay_mode    | p50_inter_arrival_ms | p95_inter_arrival_ms | p99_inter_arrival_ms | p50_send_lag_ms   | p99_send_lag_ms  |
| ----- | ----- | -------------- | -------------------- | -------------------- | -------------------- | ----------------- | ---------------- |
| Q1    | full  | sketch-snowset | 141.0                | 755.0                | 1255.0               | -568374111.144209 | -11447251.176653 |
| Q2    | full  | sketch-snowset | 10.0                 | 55.0                 | 124.0                | -599185880.037621 | -14871500.041068 |
| Q3    | full  | sketch-snowset | 22.0                 | 138.0                | 225.0                | -596232133.0369   | -17312529.68558  |
| Q4    | full  | sketch-snowset | 14.0                 | 88.0                 | 159.0                | -595647521.004381 | -15333199.642397 |
| Q5    | full  | sketch-snowset | 10.0                 | 55.0                 | 124.0                | -598998631.080416 | -14694005.692602 |

