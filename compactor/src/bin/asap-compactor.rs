//! `asap-compactor` — concat-only compactor binary.
//!
//! Walks an S3-compatible bucket under `--tenant`, plans + executes
//! compaction groups, prints metrics on stdout and a JSON summary on
//! `--out`. See the library docs in `lib.rs` for the design rationale
//! (concat-only, no decode/re-encode).

use std::sync::Arc;

use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;

use asap_compactor::{
    CompactionPlan, CompactionThresholds, Compactor, CompactorConfig, ObjectStore,
};

#[derive(Debug, Parser)]
#[command(
    name = "asap-compactor",
    about = "Concat-only S3 cold-store compactor for the ASAP GorillaQueryEngine archive tier."
)]
struct Cli {
    /// S3 endpoint URL (e.g. `http://minio:9000`). Empty ⇒ AWS S3.
    #[arg(long, env = "ASAP_GORILLA_S3_ENDPOINT", default_value = "")]
    endpoint: String,
    /// Bucket name to compact.
    #[arg(long, env = "ASAP_GORILLA_S3_BUCKET")]
    bucket: String,
    /// AWS region (or any non-empty placeholder for MinIO).
    #[arg(long, env = "ASAP_GORILLA_S3_REGION", default_value = "us-east-1")]
    region: String,
    /// Tenant prefix the compactor walks under, e.g. `tenant1`.
    #[arg(long, env = "ASAP_GORILLA_S3_TENANT", default_value = "")]
    tenant: String,
    /// AWS access key id (optional; defaults to env / IMDS).
    #[arg(long, env = "ASAP_GORILLA_S3_ACCESS_KEY_ID")]
    access_key_id: Option<String>,
    /// AWS secret access key (optional; defaults to env / IMDS).
    #[arg(long, env = "ASAP_GORILLA_S3_SECRET_ACCESS_KEY")]
    secret_access_key: Option<String>,
    /// Use HTTPS (true) or HTTP (false). MinIO over docker-compose
    /// typically wants HTTP.
    #[arg(long, default_value_t = false)]
    use_ssl: bool,
    /// Minimum number of adjacent per-hour blocks per group.
    #[arg(long, default_value_t = 6)]
    threshold_count: usize,
    /// Newest block in a group must be at least this old.
    #[arg(long, default_value_t = 6)]
    threshold_hours: i64,
    /// Print the plan but don't write anything.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    /// Skip the post-merge verify partial-read.
    #[arg(long, default_value_t = false)]
    no_verify: bool,
    /// Output JSON path for the run summary (compaction blocks +
    /// metrics snapshot). Default: `compactor.json` in cwd.
    #[arg(long, default_value = "compactor.json")]
    out: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse()?))
        .init();
    let cli = Cli::parse();

    let tenant_prefix = if cli.tenant.is_empty() {
        String::new()
    } else if cli.tenant.ends_with('/') {
        cli.tenant.clone()
    } else {
        format!("{}/", cli.tenant)
    };

    let store: Arc<dyn ObjectStore> = build_store(&cli)?;
    info!(
        bucket = cli.bucket.as_str(),
        endpoint = cli.endpoint.as_str(),
        tenant = tenant_prefix.as_str(),
        dry_run = cli.dry_run,
        threshold_count = cli.threshold_count,
        threshold_hours = cli.threshold_hours,
        "asap-compactor: starting"
    );

    let plan = CompactionPlan::discover(
        &store,
        &tenant_prefix,
        &CompactionThresholds {
            min_count: cli.threshold_count,
            min_age_hours: cli.threshold_hours,
            now: chrono::Utc::now(),
        },
    )
    .await?;
    info!(
        groups = plan.groups.len(),
        deferred = plan.deferred.len(),
        "asap-compactor: discovered plan"
    );

    let compactor = Compactor::new(
        store.clone(),
        CompactorConfig {
            dry_run: cli.dry_run,
            verify: !cli.no_verify,
            tenant_prefix,
        },
    );
    let result = compactor.run(&plan).await?;

    // Write a JSON summary so the demo can render the before/after
    // numbers without re-listing the bucket.
    let summary = serde_json::json!({
        "dry_run": cli.dry_run,
        "groups_planned": plan.groups.len(),
        "blocks_emitted": result.blocks.len(),
        "deferred": plan.deferred.len(),
        "metrics": {
            "blocks_in": result.metrics.blocks_in,
            "blocks_out": result.metrics.blocks_out,
            "chunks_in": result.metrics.chunks_in,
            "chunks_out": result.metrics.chunks_out,
            "bytes_in": result.metrics.bytes_in,
            "bytes_out": result.metrics.bytes_out,
            "s3_put_count": result.metrics.s3_put_count,
            "s3_get_count": result.metrics.s3_get_count,
            "s3_delete_count": result.metrics.s3_delete_count,
            "duration_ms": result.metrics.duration_ms,
        },
        "blocks": result.blocks.iter().map(|b| serde_json::json!({
            "merged_chunk_key": b.merged_chunk_key,
            "merged_index_key": b.merged_index_key,
            "merged_postings_key": b.merged_postings_key,
            "merged_chunk_bytes": b.merged_chunk_bytes,
            "chunks_merged": b.chunks_merged,
            "source_blocks_deleted": b.source_blocks_deleted,
        })).collect::<Vec<_>>(),
    });
    std::fs::write(&cli.out, serde_json::to_string_pretty(&summary)?)?;
    info!(out = cli.out.as_str(), "asap-compactor: wrote summary");
    println!("{}", compactor.metrics().render_prometheus());
    Ok(())
}

fn build_store(cli: &Cli) -> anyhow::Result<Arc<dyn ObjectStore>> {
    use s3::creds::Credentials;
    use s3::region::Region as S3Region;
    use s3::Bucket;

    let region = if cli.endpoint.is_empty() {
        cli.region
            .parse::<S3Region>()
            .map_err(|e| anyhow::anyhow!("region parse: {e}"))?
    } else {
        let endpoint = if cli.endpoint.starts_with("http://") || cli.endpoint.starts_with("https://") {
            cli.endpoint.clone()
        } else if cli.use_ssl {
            format!("https://{}", cli.endpoint)
        } else {
            format!("http://{}", cli.endpoint)
        };
        S3Region::Custom {
            region: cli.region.clone(),
            endpoint,
        }
    };
    let creds = match (&cli.access_key_id, &cli.secret_access_key) {
        (Some(ak), Some(sk)) => Credentials::new(Some(ak), Some(sk), None, None, None)?,
        _ => Credentials::default()?,
    };
    let bucket = Bucket::new(&cli.bucket, region, creds)?;
    let bucket = if cli.endpoint.is_empty() {
        bucket
    } else {
        bucket.with_path_style()
    };
    Ok(Arc::new(S3RealObjectStore { bucket }))
}

/// `rust-s3`-backed [`ObjectStore`] for production use.
struct S3RealObjectStore {
    bucket: Box<s3::Bucket>,
}

#[async_trait::async_trait]
impl ObjectStore for S3RealObjectStore {
    async fn list_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, asap_compactor::ObjectStoreError> {
        let pages = self
            .bucket
            .list(prefix.to_string(), None)
            .await
            .map_err(|e| asap_compactor::ObjectStoreError::Backend(format!("s3 list {prefix}: {e}")))?;
        let mut out = Vec::new();
        for page in pages {
            for obj in page.contents {
                out.push(obj.key);
            }
        }
        Ok(out)
    }

    async fn get_object(&self, key: &str) -> Result<Vec<u8>, asap_compactor::ObjectStoreError> {
        let resp = self
            .bucket
            .get_object(key)
            .await
            .map_err(|e| asap_compactor::ObjectStoreError::Backend(format!("s3 get {key}: {e}")))?;
        if resp.status_code() == 404 {
            return Err(asap_compactor::ObjectStoreError::NotFound(key.to_string()));
        }
        if !(200..300).contains(&resp.status_code()) {
            return Err(asap_compactor::ObjectStoreError::Backend(format!(
                "s3 get {key}: status {}",
                resp.status_code()
            )));
        }
        Ok(resp.to_vec())
    }

    async fn get_range(
        &self,
        key: &str,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, asap_compactor::ObjectStoreError> {
        let end = offset + length - 1;
        let resp = self
            .bucket
            .get_object_range(key, offset, Some(end))
            .await
            .map_err(|e| {
                asap_compactor::ObjectStoreError::Backend(format!(
                    "s3 range get {key} {offset}..{length}: {e}"
                ))
            })?;
        if resp.status_code() == 404 {
            return Err(asap_compactor::ObjectStoreError::NotFound(key.to_string()));
        }
        if !(200..300).contains(&resp.status_code()) {
            return Err(asap_compactor::ObjectStoreError::Backend(format!(
                "s3 range get {key}: status {}",
                resp.status_code()
            )));
        }
        Ok(resp.to_vec())
    }

    async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<(), asap_compactor::ObjectStoreError> {
        self.bucket
            .put_object(key, &body)
            .await
            .map_err(|e| asap_compactor::ObjectStoreError::Backend(format!("s3 put {key}: {e}")))?;
        Ok(())
    }

    async fn delete_object(&self, key: &str) -> Result<(), asap_compactor::ObjectStoreError> {
        self.bucket
            .delete_object(key)
            .await
            .map_err(|e| asap_compactor::ObjectStoreError::Backend(format!("s3 delete {key}: {e}")))?;
        Ok(())
    }

    async fn object_exists(
        &self,
        key: &str,
    ) -> Result<bool, asap_compactor::ObjectStoreError> {
        let (_data, code) = self
            .bucket
            .head_object(key)
            .await
            .map_err(|e| asap_compactor::ObjectStoreError::Backend(format!("s3 head {key}: {e}")))?;
        Ok(code != 404)
    }
}
