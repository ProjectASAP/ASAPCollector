# ASAPCollector documentation

The documentation is grouped by the question it answers:

- [Design docs](design_docs/README.md) explain system behavior, constraints, and trade-offs.
- [Developer docs](developer_docs/README.md) describe supported extension points and how to validate changes.
- [User guide](user_guide/README.md) contains commands for setup, operation, and end-to-end checks.

## Canonical architecture hub

Read the [system overview](design_docs/cross-cutting/system-overview.md) for the end-to-end model,
then use the design and developer indexes to find one primary page per component.
This directory owns the cross-system architecture for ASAPCollector,
ASAPQuery-backend, and their control-plane contracts. ASAPQuery-backend keeps
code-level implementation detail beside its source under its own consolidated
`docs/` directory; these pages link to it rather than duplicating private internals.
