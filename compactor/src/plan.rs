//! Compaction planner — discovers groups of small per-hour blocks
//! that can be merged. Pure logic; no I/O beyond `ObjectStore::list_prefix`.
//!
//! ## Threshold rules (mvp/v5)
//!
//! A group of `≥ threshold_count` blocks for the same `(tenant,
//! metric)` whose newest member is older than `threshold_hours` is
//! eligible. Both knobs are `--threshold-count` (default 6) and
//! `--threshold-hours` (default 6) on the binary; the planner takes
//! them as a [`CompactionThresholds`] struct so library callers
//! (the upcoming controller integration) can drive it directly.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::object_store::{ObjectStore, ObjectStoreError};

/// Tunable compaction thresholds. Both apply per-group; only groups
/// that satisfy *both* are emitted by [`CompactionPlan::discover`].
#[derive(Debug, Clone)]
pub struct CompactionThresholds {
    /// Minimum number of adjacent per-hour blocks in a group.
    /// Default: `6`.
    pub min_count: usize,
    /// Newest block in the group must be older than this many hours.
    /// Default: `6`.
    pub min_age_hours: i64,
    /// Wall-clock "now" for the age threshold. Configurable so
    /// tests can pin it without manipulating the system clock.
    pub now: DateTime<Utc>,
}

impl Default for CompactionThresholds {
    fn default() -> Self {
        Self {
            min_count: 6,
            min_age_hours: 6,
            now: Utc::now(),
        }
    }
}

/// One source block discovered by [`CompactionPlan::discover`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceBlock {
    /// Tenant slug from the prefix template, e.g. `"tenant1"`.
    pub tenant: String,
    /// Metric name from the prefix template, e.g. `"http_requests_total"`.
    pub metric: String,
    /// `YYYY/MM/DD/HH` hour bucket the block belongs to.
    pub hour_prefix: String,
    /// Full S3 key of the `index.json` for this block. The other two
    /// (`postings-v1.json`, the chunk(s) themselves) live at the
    /// same key prefix and are derived at compaction time.
    pub index_key: String,
}

impl SourceBlock {
    /// S3 key prefix (always ends with `/`) under which all of the
    /// block's objects live.
    pub fn prefix(&self) -> String {
        let mut k = self.index_key.clone();
        // strip "index.json" — should always be the suffix.
        if let Some(stripped) = k.strip_suffix("index.json") {
            k = stripped.to_string();
        }
        k
    }
}

/// One group of source blocks that the compactor will merge into a
/// single output block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionGroup {
    /// Tenant + metric this group belongs to. All sources share both.
    pub tenant: String,
    /// See [`Self::tenant`].
    pub metric: String,
    /// Source blocks in time-ascending order.
    pub sources: Vec<SourceBlock>,
    /// Target output prefix the merged block will be written under.
    /// Conventionally `<tenant>/<metric>/day=YYYY-MM-DD/`.
    pub output_prefix: String,
    /// Output object base name (no extension): the binary appends
    /// `.gor` / `.index.json` / `.postings-v1.json`.
    pub output_basename: String,
}

/// Top-level plan returned by [`CompactionPlan::discover`].
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionPlan {
    /// Groups eligible for compaction under the supplied thresholds.
    pub groups: Vec<CompactionGroup>,
    /// Source blocks that were too new (or too few in their bucket)
    /// to be eligible. Useful for the `--dry-run` plan summary.
    pub deferred: Vec<SourceBlock>,
}

impl CompactionPlan {
    /// Walk `bucket` under `tenant_prefix` (e.g. `"tenant1/"`) and
    /// build a plan honoring `thresholds`. The planner is purely
    /// list-based: it enumerates all `index.json` keys, parses the
    /// (tenant, metric, hour) tuple out of the path, sorts by
    /// (metric, hour), and walks adjacent runs.
    pub async fn discover(
        store: &Arc<dyn ObjectStore>,
        tenant_prefix: &str,
        thresholds: &CompactionThresholds,
    ) -> Result<Self, ObjectStoreError> {
        let prefix = if tenant_prefix.is_empty() || tenant_prefix.ends_with('/') {
            tenant_prefix.to_string()
        } else {
            format!("{tenant_prefix}/")
        };
        let keys = store.list_prefix(&prefix).await?;

        // Find every index.json (one per hour-bucket) and parse out
        // (tenant, metric, hour_prefix). Anything we can't parse is
        // logged + skipped.
        let mut by_metric: BTreeMap<(String, String), Vec<SourceBlock>> = BTreeMap::new();
        for key in keys {
            // Skip postings + chunks; only index.json drives the
            // discovery walk. Compactor outputs (under `day=…/`)
            // are also skipped — those have already been merged.
            if !key.ends_with("/index.json") {
                continue;
            }
            if key.contains("/day=") {
                // already-compacted output (merged blocks live
                // under `day=YYYY-MM-DD/`); skip to keep the
                // compactor idempotent.
                continue;
            }
            let Some(parsed) = parse_block_key(&key) else {
                debug!(key = %key, "compactor planner: could not parse block key; skipping");
                continue;
            };
            by_metric.entry((parsed.tenant.clone(), parsed.metric.clone()))
                .or_default()
                .push(parsed);
        }

        let mut plan = Self::default();
        for ((tenant, metric), mut blocks) in by_metric {
            // Sort by hour_prefix (lexicographic on YYYY/MM/DD/HH is
            // the same as time-ascending).
            blocks.sort_by(|a, b| a.hour_prefix.cmp(&b.hour_prefix));

            // Find adjacency-runs; for the MVP we treat the whole
            // metric's block list as one run if all members satisfy
            // both thresholds. (A multi-day backlog would form
            // multiple groups in production; the controller can
            // chunk by day before invoking us.)
            let mut group_buf: Vec<SourceBlock> = Vec::new();
            for blk in &blocks {
                let age_ok = block_age_hours(thresholds.now, &blk.hour_prefix)
                    .map(|h| h >= thresholds.min_age_hours)
                    .unwrap_or(false);
                if age_ok {
                    group_buf.push(blk.clone());
                } else {
                    plan.deferred.push(blk.clone());
                }
            }

            // Emit groups of `min_count` consecutive blocks. The
            // remainder (less than `min_count`) gets deferred so a
            // future run with more blocks can pick them up.
            let mut i = 0;
            while i + thresholds.min_count <= group_buf.len() {
                let chunk = &group_buf[i..i + thresholds.min_count];
                let output_basename = format!(
                    "block-{:04}-{:04}",
                    i,
                    i + thresholds.min_count - 1
                );
                let day_prefix = day_prefix_from_hour(&chunk[0].hour_prefix)
                    .unwrap_or_else(|| "day=unknown".to_string());
                let output_prefix = format!(
                    "{tenant}/{metric}/{day_prefix}/"
                );
                plan.groups.push(CompactionGroup {
                    tenant: tenant.clone(),
                    metric: metric.clone(),
                    sources: chunk.to_vec(),
                    output_prefix,
                    output_basename,
                });
                i += thresholds.min_count;
            }
            // Anything left over is deferred (will be picked up in
            // a later compactor run when the backlog grows).
            for blk in &group_buf[i..] {
                plan.deferred.push(blk.clone());
            }
        }
        Ok(plan)
    }
}

/// Parse a block index key like
/// `"tenant1/http_requests_total/2026/05/06/12/index.json"` into a
/// [`SourceBlock`]. Returns `None` for keys that don't fit the shape.
fn parse_block_key(key: &str) -> Option<SourceBlock> {
    let stripped = key.strip_suffix("/index.json")?;
    // tenant/metric/YYYY/MM/DD/HH
    let parts: Vec<&str> = stripped.split('/').collect();
    if parts.len() < 6 {
        return None;
    }
    let n = parts.len();
    let hour = parts[n - 1];
    let day = parts[n - 2];
    let month = parts[n - 3];
    let year = parts[n - 4];
    if !(year.len() == 4 && month.len() == 2 && day.len() == 2 && hour.len() == 2) {
        return None;
    }
    let metric = parts[n - 5];
    let tenant = parts[..n - 5].join("/");
    Some(SourceBlock {
        tenant,
        metric: metric.to_string(),
        hour_prefix: format!("{year}/{month}/{day}/{hour}"),
        index_key: key.to_string(),
    })
}

/// Hour-bucket → wall-clock age in hours. Returns `None` for unparseable
/// `YYYY/MM/DD/HH`.
fn block_age_hours(now: DateTime<Utc>, hour_prefix: &str) -> Option<i64> {
    let parts: Vec<&str> = hour_prefix.split('/').collect();
    if parts.len() != 4 {
        return None;
    }
    let year: i32 = parts[0].parse().ok()?;
    let month: u32 = parts[1].parse().ok()?;
    let day: u32 = parts[2].parse().ok()?;
    let hour: u32 = parts[3].parse().ok()?;
    use chrono::TimeZone;
    let dt = Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).single()?;
    Some((now - dt).num_hours())
}

/// `"2026/05/06/12"` → `"day=2026-05-06"`.
fn day_prefix_from_hour(hour_prefix: &str) -> Option<String> {
    let parts: Vec<&str> = hour_prefix.split('/').collect();
    if parts.len() != 4 {
        return None;
    }
    Some(format!("day={}-{}-{}", parts[0], parts[1], parts[2]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn now_utc() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 7, 0, 0, 0).unwrap()
    }

    #[test]
    fn parse_block_key_happy_path() {
        let key = "tenant1/http_requests_total/2026/05/06/12/index.json";
        let parsed = parse_block_key(key).unwrap();
        assert_eq!(parsed.tenant, "tenant1");
        assert_eq!(parsed.metric, "http_requests_total");
        assert_eq!(parsed.hour_prefix, "2026/05/06/12");
        assert_eq!(parsed.index_key, key);
    }

    #[test]
    fn parse_block_key_rejects_short() {
        assert!(parse_block_key("tenant1/index.json").is_none());
    }

    #[test]
    fn block_age_hours_basic() {
        let now = Utc.with_ymd_and_hms(2026, 5, 6, 18, 0, 0).unwrap();
        assert_eq!(block_age_hours(now, "2026/05/06/12"), Some(6));
        assert_eq!(block_age_hours(now, "2026/05/06/00"), Some(18));
    }

    #[tokio::test]
    async fn discover_groups_six_old_blocks_per_metric() {
        use crate::object_store::InMemoryObjectStore;
        let store = Arc::new(InMemoryObjectStore::new());
        // Six 24-hour-old blocks for one metric.
        for hour in 0u32..6 {
            let key = format!(
                "tenant1/m/2026/05/06/{:02}/index.json",
                hour
            );
            store.put_object(&key, b"{}".to_vec()).await.unwrap();
        }
        let store: Arc<dyn ObjectStore> = store;
        let plan = CompactionPlan::discover(
            &store,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_utc(),
            },
        )
        .await
        .unwrap();
        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.groups[0].sources.len(), 6);
        assert_eq!(plan.groups[0].metric, "m");
        assert_eq!(plan.groups[0].output_prefix, "tenant1/m/day=2026-05-06/");
        assert_eq!(plan.groups[0].output_basename, "block-0000-0005");
        assert!(plan.deferred.is_empty());
    }

    #[tokio::test]
    async fn discover_skips_too_new_blocks() {
        use crate::object_store::InMemoryObjectStore;
        let store = Arc::new(InMemoryObjectStore::new());
        // Six blocks from "today", which is < 6 hours old at 00:00.
        for hour in 0u32..6 {
            let key = format!(
                "tenant1/m/2026/05/07/{:02}/index.json",
                hour
            );
            store.put_object(&key, b"{}".to_vec()).await.unwrap();
        }
        let store: Arc<dyn ObjectStore> = store;
        let plan = CompactionPlan::discover(
            &store,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_utc(),
            },
        )
        .await
        .unwrap();
        assert!(plan.groups.is_empty(), "all blocks too new");
        assert_eq!(plan.deferred.len(), 6);
    }

    #[tokio::test]
    async fn discover_skips_already_compacted_output() {
        // A previously-compacted output (under `day=…/`) must NOT
        // be re-discovered as a source — that's the idempotency
        // contract.
        use crate::object_store::InMemoryObjectStore;
        let store = Arc::new(InMemoryObjectStore::new());
        store
            .put_object(
                "tenant1/m/day=2026-05-06/block-0000-0005.index.json",
                b"{}".to_vec(),
            )
            .await
            .unwrap();
        let store: Arc<dyn ObjectStore> = store;
        let plan = CompactionPlan::discover(
            &store,
            "tenant1/",
            &CompactionThresholds {
                min_count: 6,
                min_age_hours: 6,
                now: now_utc(),
            },
        )
        .await
        .unwrap();
        assert!(plan.groups.is_empty());
        assert!(plan.deferred.is_empty());
    }
}
