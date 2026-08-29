# Developing the summary transmission protocol

The sender emits Schema, Dictionary, and Record information at different rates.
Schema identifies aggregation kind, parameters, encoding, and schema version;
Dictionary binds a stream-local reference to SID/metric/retained labels; Record
contains SID reference, window bounds, sequence/base identity, and full or delta
payload. A receiver must be able to select a decoder without inspecting opaque bytes.

For full mode, each record is independently decodable. For delta mode, the sender
maintains the acknowledged base per `(destination, SID, epoch)` and a monotonically
ordered sequence; the receiver maintains the applied base/sequence for the same key.
Lost base, gap, restart, or replica handoff requires resynchronization with a full
snapshot. Retries are idempotent; applying a delta twice is forbidden.

Test full/delta round trips, schema incompatibility, dictionary replay, gaps,
duplicates, out-of-order records, sender and receiver restart, replica handoff,
forced full resync, and multi-SID isolation. The normative semantics are in
[the transmission design](../design_docs/collector-transmission-protocol.md).

