//! HTTP client that pushes a freshly-generated `StreamingConfig` YAML
//! to the ASAPQuery-backend's `POST /api/v1/streaming-config` endpoint.
//!
//! This is the controller-side **producer** of the PR E phase 1 / phase 2
//! hot-reload contract that landed in ASAPQuery-backend PRs #10 and #12.
//! The replanner calls into this module immediately after generating a
//! new plan so the backend's active `StreamingConfig` is updated without
//! a restart and subsequent queries observe the new aggregation layout.
//!
//! The client is **fire-and-forget at the call site** — the replanner
//! awaits the POST but doesn't block its own return on the outcome.
//! Errors are logged at WARN; the controller is expected to be tolerant
//! of transient backend unavailability because the next replan cycle
//! will try again with the latest plan.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{debug, warn};

/// Minimal HTTP client for ASAPQuery-backend's streaming-config endpoint.
/// Built once at controller startup from the `CONTROLLER_BACKEND_ENDPOINT`
/// environment variable and shared via `Arc` with the replanner.
#[derive(Debug, Clone)]
pub struct BackendClient {
    endpoint: String,
    http: Client,
}

impl BackendClient {
    /// Construct a client pointing at the backend's plan-push endpoint.
    /// `endpoint` should be the full URL, e.g.
    /// `http://backend.svc:8088/api/v1/streaming-config`.
    ///
    /// A 5-second timeout bounds the duration a slow or unreachable
    /// backend can stall the replanner — consistent with the symmetric
    /// 5-second timeout on ASAPQuery-backend's `HttpControllerClient`
    /// (the reverse direction in the same loop).
    pub fn new(endpoint: impl Into<String>) -> Self {
        let http = Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            endpoint: endpoint.into(),
            http,
        }
    }

    /// Construct with an explicit `reqwest::Client`. Used by tests that
    /// need to inject a mock-server URL without reconfiguring the
    /// timeout setup.
    pub fn with_http(endpoint: impl Into<String>, http: Client) -> Self {
        Self {
            endpoint: endpoint.into(),
            http,
        }
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// POST the given `StreamingConfig` YAML to the backend. Returns
    /// `Ok(())` on any 2xx status, otherwise an error carrying the
    /// status code and response body. The caller (typically
    /// [`Replanner::replan_metric`]) logs the error and moves on — the
    /// next replan cycle will retry with the latest plan.
    pub async fn push_streaming_config(&self, yaml: String) -> Result<()> {
        debug!(
            endpoint = %self.endpoint,
            yaml_bytes = yaml.len(),
            "pushing streaming-config YAML to ASAPQuery-backend"
        );
        let resp = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/x-yaml")
            .body(yaml)
            .send()
            .await
            .context("failed to POST streaming-config to backend")?;

        let status = resp.status();
        if status.is_success() {
            Ok(())
        } else {
            let body = resp.text().await.unwrap_or_default();
            Err(anyhow::anyhow!(
                "backend returned {} for streaming-config POST: {}",
                status,
                body
            ))
        }
    }
}

/// Fire-and-forget convenience helper used by the replanner. Logs
/// errors at WARN and never propagates them — the replanner should
/// never fail an entire replan because the backend was temporarily
/// unreachable.
pub async fn push_or_log(client: &BackendClient, metric: &str, yaml: String) {
    match client.push_streaming_config(yaml).await {
        Ok(()) => {
            debug!(metric, endpoint = %client.endpoint, "streaming-config push succeeded");
        }
        Err(e) => {
            warn!(
                metric,
                endpoint = %client.endpoint,
                error = %e,
                "streaming-config push to ASAPQuery-backend failed; \
                 next replan cycle will retry"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::routing::post;
    use axum::Router;
    use std::sync::{Arc as StdArc, Mutex};

    #[derive(Clone)]
    struct SharedSink(StdArc<Mutex<Vec<String>>>);

    async fn start_mock_backend(sink: SharedSink, status: axum::http::StatusCode) -> String {
        let app = Router::new()
            .route(
                "/api/v1/streaming-config",
                post(
                    move |State(sink): State<SharedSink>, body: axum::body::Bytes| async move {
                        let yaml = String::from_utf8_lossy(&body).to_string();
                        sink.0.lock().unwrap().push(yaml);
                        status
                    },
                ),
            )
            .with_state(sink);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("http://{addr}/api/v1/streaming-config")
    }

    #[tokio::test]
    async fn success_path_round_trips_yaml() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url = start_mock_backend(sink.clone(), axum::http::StatusCode::OK).await;

        let client = BackendClient::new(url);
        let yaml = "aggregations:\n  - aggregationId: 42\n    metric: cpu\n".to_string();
        client
            .push_streaming_config(yaml.clone())
            .await
            .expect("push ok");

        let received = sink.0.lock().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0], yaml);
    }

    #[tokio::test]
    async fn non_2xx_status_is_reported_as_error() {
        let sink = SharedSink(StdArc::new(Mutex::new(Vec::new())));
        let url =
            start_mock_backend(sink.clone(), axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;

        let client = BackendClient::new(url);
        let result = client.push_streaming_config("whatever".to_string()).await;
        assert!(result.is_err(), "expected error on 500, got {result:?}");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("500"), "error msg should mention 500: {msg}");
    }

    #[tokio::test]
    async fn push_or_log_swallows_errors() {
        // Point at an unreachable port so the request fails fast.
        let client = BackendClient::new("http://127.0.0.1:1/api/v1/streaming-config");
        // Must not panic or propagate — fire-and-forget semantics.
        push_or_log(&client, "cpu_usage", "content".to_string()).await;
    }
}
