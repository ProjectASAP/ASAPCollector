//! Per-series sketch payload cache for delta encoding (outbound) and
//! inbound delta-apply against cached upstream snapshots.
//!
//! Mirrors `asap-precompute-go/snapshot_cache.go` and today's
//! per-processor `snapshots map[string][]byte` (outbound) +
//! `IngestState::sketch_snapshots` (inbound).

use std::collections::HashMap;
use std::sync::RwLock;

use crate::precompute::{DeltaResult, PrecomputeError, Sketch};

/// Stores per-series sketch payloads for delta encoding (outbound)
/// and inbound delta-apply against cached upstream snapshots.
///
/// Mirrors Go `precompute.SnapshotCache`.
///
/// Two maps avoid cross-direction collisions: the same series key
/// may be both produced (outbound) and consumed (inbound) by the
/// same host when running as a forwarder, and the two byte streams
/// are not interchangeable (a remote sender's snapshot is not what
/// we'd emit locally).
///
/// **Always-refresh policy** (mirrors the Go side / ADR-0002
/// §"Behavior preservation"): every call to
/// [`Self::compute_delta`] updates the cached previous snapshot to
/// the current sketch state. Successive sub-threshold deltas are
/// therefore each computed against the immediately preceding
/// window, matching the established behavior of all five legacy
/// OTel sketch processors (DDSketch / KLL / HLL / CountSketch /
/// CountMinSketch). There is no configurable
/// "refresh-only-on-full" mode — that earlier design was a bug
/// because it forced downstream consumers to merge a chain of
/// deltas back to the original baseline rather than apply each
/// delta to the previous window's reconstructed state.
pub struct SnapshotCache {
    inner: RwLock<SnapshotCacheInner>,
}

struct SnapshotCacheInner {
    outbound: HashMap<String, Vec<u8>>,
    inbound: HashMap<String, Vec<u8>>,
}

impl Default for SnapshotCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotCache {
    /// Constructs an empty cache. Mirrors Go `NewSnapshotCache`.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(SnapshotCacheInner {
                outbound: HashMap::new(),
                inbound: HashMap::new(),
            }),
        }
    }

    /// Stores the latest full sketch payload, keyed by `series_key`.
    /// Returns `true` if this is the first snapshot for that key
    /// (caller can use this to force a [`crate::envelope::Encoding::ProtoFull`]
    /// on next emit).
    ///
    /// Stores a defensive copy of `payload` so a caller mutating its
    /// slice after caching does not race the cache reader. Mirrors
    /// Go `(*SnapshotCache).CacheOutbound`.
    pub fn cache_outbound(&self, series_key: &str, payload: &[u8]) -> bool {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        let first_time = !g.outbound.contains_key(series_key);
        g.outbound.insert(series_key.to_string(), payload.to_vec());
        first_time
    }

    /// Returns the cached outbound payload, or `None`. Mirrors Go
    /// `(*SnapshotCache).GetOutbound`.
    pub fn get_outbound(&self, series_key: &str) -> Option<Vec<u8>> {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .get(series_key)
            .cloned()
    }

    /// Stores an upstream snapshot for delta apply. Mirrors Go
    /// `(*SnapshotCache).CacheInbound`.
    pub fn cache_inbound(&self, series_key: &str, payload: &[u8]) {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        g.inbound.insert(series_key.to_string(), payload.to_vec());
    }

    /// Returns the cached upstream snapshot or `None`. Mirrors Go
    /// `(*SnapshotCache).GetInbound`.
    pub fn get_inbound(&self, series_key: &str) -> Option<Vec<u8>> {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .inbound
            .get(series_key)
            .cloned()
    }

    /// Diffs current sketch state against the cached outbound
    /// snapshot for `series_key`.
    ///
    /// Returns a [`DeltaResult`] where `is_full` means the runtime
    /// should emit `ProtoFull` (either no prior snapshot existed,
    /// or the delta exceeded `threshold`). Mirrors Go
    /// `(*SnapshotCache).ComputeDelta`.
    ///
    /// Always-refresh: every call updates the cached previous
    /// snapshot to the current sketch state. When `is_full=true`
    /// the wire payload IS the full snapshot, so it is reused for
    /// the cache; otherwise a fresh full snapshot is serialized
    /// for the cache. Both branches end with the cache holding
    /// the latest full state.
    pub fn compute_delta(
        &self,
        series_key: &str,
        current: &dyn Sketch,
        threshold: u64,
    ) -> Result<DeltaResult, PrecomputeError> {
        let prev = self
            .inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .get(series_key)
            .cloned();

        let result = match prev {
            None => {
                // First time — emit full.
                let full = current.snapshot()?;
                DeltaResult {
                    payload: full,
                    is_full: true,
                }
            }
            Some(prev_bytes) => current.compute_delta_against(&prev_bytes, threshold)?,
        };

        // Always-refresh: update the cached outbound to the latest
        // full snapshot. When is_full=true the wire payload IS the
        // snapshot; reuse it. Otherwise serialize a fresh snapshot.
        if result.is_full {
            self.cache_outbound(series_key, &result.payload);
        } else {
            let full = current.snapshot()?;
            self.cache_outbound(series_key, &full);
        }
        Ok(result)
    }

    /// Clears all cached state. Used in tests and on shutdown.
    /// Mirrors Go `(*SnapshotCache).Reset`.
    pub fn reset(&self) {
        let mut g = self.inner.write().expect("snapshot cache poisoned");
        g.outbound.clear();
        g.inbound.clear();
    }

    /// Returns the number of cached outbound snapshots. Mirrors Go
    /// `(*SnapshotCache).LenOutbound`.
    pub fn len_outbound(&self) -> usize {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .outbound
            .len()
    }

    /// Returns the number of cached inbound snapshots. Mirrors Go
    /// `(*SnapshotCache).LenInbound`.
    pub fn len_inbound(&self) -> usize {
        self.inner
            .read()
            .expect("snapshot cache poisoned")
            .inbound
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_first_time_true_then_false() {
        let c = SnapshotCache::new();
        assert!(c.cache_outbound("series-1", &[1, 2, 3]));
        assert!(!c.cache_outbound("series-1", &[4, 5, 6]));
        assert_eq!(c.len_outbound(), 1);
        assert_eq!(c.get_outbound("series-1"), Some(vec![4, 5, 6]));
    }

    #[test]
    fn inbound_independent_from_outbound() {
        let c = SnapshotCache::new();
        c.cache_outbound("k", b"out");
        c.cache_inbound("k", b"in");
        assert_eq!(c.get_outbound("k"), Some(b"out".to_vec()));
        assert_eq!(c.get_inbound("k"), Some(b"in".to_vec()));
        assert_eq!(c.len_outbound(), 1);
        assert_eq!(c.len_inbound(), 1);
    }

    #[test]
    fn reset_clears_all() {
        let c = SnapshotCache::new();
        c.cache_outbound("k", b"v");
        c.cache_inbound("k", b"v");
        c.reset();
        assert_eq!(c.len_outbound(), 0);
        assert_eq!(c.len_inbound(), 0);
    }

    #[test]
    fn cache_stores_defensive_copy() {
        let c = SnapshotCache::new();
        let mut payload = vec![1, 2, 3];
        c.cache_outbound("k", &payload);
        payload[0] = 99;
        // Cache must not have mutated.
        assert_eq!(c.get_outbound("k"), Some(vec![1, 2, 3]));
    }
}
