# Developing workload collection

Emit query and data workload observations as separate versioned schemas. Include the
measurement interval, tenant/source, sampling method, and confidence or completeness
information. Normalize query syntax before aggregation, but retain semantic features
such as grouping, range, operation, accuracy, and freshness. Data profiles must retain
metric identity, cardinalities, rates, distributions, lateness, retention, and
locality. Test schema evolution, duplicate intervals, missing fields, sampled counts,
tenant isolation, and deterministic snapshot generation.

