# Design documents

These documents define ASAPCollector's system boundary, collector-specific
control-plane contract, summary semantics, and explicitly deferred designs.
They describe goals, behavior, constraints, trade-offs, and acceptance
conditions. Code walkthroughs, implementation plans, benchmark results, and
paper-planning material do not belong in this directory.

## Active MVP design

- [System overview](system-overview.md) — end-to-end architecture, system
  boundary, goals, and MVP claims.
- [ASAPCollector control plane](control-plane-design.md) — the collector-side
  contract for plans issued by the ASAPQuery-backend control plane using
  planning information from ASAPPlanner.
- [Summary aggregation and transmission](summary-aggregation-and-transmission.md)
  — summary families, aggregation shapes, windows, accuracy, freshness, and
  raw/full/delta transmission semantics.

## Future design

- [Future summary families and compression](future-summary-and-compression.md)
  — deferred multivariate summaries and raw/archive compression.

Every document begins with a TL;DR, lifecycle status, and MVP relationship.
The overview owns system architecture; focused documents define only their
named scope and avoid repeating the full architecture.
