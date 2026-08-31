# Developing the Collector collection phase

Trace a rule through configuration decode, validation, staged installation,
observation routing, keyed state, window rotation, encoding, and export. Keep the
plan version and SID visible at every seam. Configuration validation must reject
unknown algorithms, invalid windows, unsupported encodings, duplicate conflicting
SIDs, and incompatible full/delta settings before state is allocated.

For changes, test: stable grouping under reordered labels; independent state per SID;
boundary and late timestamps; atomic plan replacement; drain on retirement; resource
limits; duplicate config delivery; export retry; and restart behavior. The concrete
control transport is covered by [OpAMP config push](opamp-config-push.md).

