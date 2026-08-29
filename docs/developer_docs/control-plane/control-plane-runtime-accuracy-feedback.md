# Developing runtime accuracy feedback

An evaluator records candidate and reference result identity before comparing them.
Evidence without matching tenant, query class, interval, grouping, and plan/SID
version is discarded. Aggregate error distributions, coverage, freshness, latency,
and sample count; do not report a single average as a guarantee. Test reference
unavailability, low sample counts, incompatible versions, privacy/retention limits,
delayed evidence, deduplication, hysteresis, cooldown, and reproducible replanning
inputs.

