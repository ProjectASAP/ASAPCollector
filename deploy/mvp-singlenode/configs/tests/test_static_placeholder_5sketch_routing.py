"""Smoke test: static agent placeholder YAML carries the canonical
5-sketch routing-connector pipeline shape.

This test exists because PR #350 flagged a real OpAMP-push gap: the
controller's typed bootstrap (`emit_edge_yaml_5sketch_routing`,
controller/src/config/stage_config.rs) emits the 5-sketch routing-
connector form correctly, but the agent never APPLIES the OpAMP
RemoteConfig push at runtime. The effective-config preview keeps
showing the static bootstrap YAML loaded from the volume mount, so the
running pipeline is whatever this static file declares.

Previously the static placeholder
(`asap-otel-agent-b6-asap-single-sketch.yaml`) declared a single
`[gorillas3, ddsketch, batch]` pipeline, so the running agent only
ever ran ONE sketch (DDSketch) regardless of the controller plan.
KLL / HLL / CountSketch / CountMinSketch processors never ran.

This test pins the new static placeholder shape so a regression in
`deploy/mvp-singlenode/configs/asap-otel-agent-b6-asap-single-sketch.yaml` (the
default `AGENT_CONFIG_A` / `AGENT_CONFIG_B` in
`deploy/mvp-singlenode/docker-compose/mvp-multi-stage.yml`) is caught by the smoke
suite before it lands on agents.

Mirrors the controller-side asserts at
`controller/src/config/stage_config.rs::tests::mvp46_*`.

Run:
    python3 deploy/mvp-singlenode/configs/tests/test_static_placeholder_5sketch_routing.py
"""

import os
import sys
import unittest

import yaml


REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, os.pardir)
)
PLACEHOLDER_PATH = os.path.join(
    REPO_ROOT,
    "deploy",
    "mvp-singlenode",
    "configs",
    "asap-otel-agent-b6-asap-single-sketch.yaml",
)

# Per-family processor names — these are the IDs the patched contrib
# build's `factory.go` actually registers (verified by
# `asap-otel components`). The Rust controller emit aliases these as
# `ddsketchprocessor` / `kllprocessor` / etc., but those aren't
# accepted by the agent's component registry and would fail the
# `validate` check; the static placeholder uses the registered names
# so it ACTUALLY LOADS.
SKETCH_PROCESSORS = [
    "ddsketch",
    "KLL",
    "HLL",
    "countsketch",
    "countmin",
]

# Per-family pipeline names — keep in sync with
# `controller/src/config/stage_config.rs::sketch_kind_to_pipeline_name`.
SKETCH_PIPELINES = [
    "metrics/ddsketch_path",
    "metrics/kll_path",
    "metrics/hll_path",
    "metrics/countsketch_path",
    "metrics/countminsketch_path",
]


def load_placeholder():
    with open(PLACEHOLDER_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


class TestStaticPlaceholder5SketchRouting(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.doc = load_placeholder()

    # 1. All 5 sketch processors present ────────────────────────────────
    def test_all_5_sketch_processors_present(self):
        processors = self.doc.get("processors", {})
        for name in SKETCH_PROCESSORS:
            self.assertIn(
                name,
                processors,
                f"missing top-level processor key {name!r} — runtime swap "
                f"needs all 5 families pre-loaded",
            )

    # 2. routing lives under connectors:, NOT processors: ──────────────
    def test_routing_in_connectors_not_processors(self):
        # The real bugfix: OTel collector v0.106+ removed
        # `routingprocessor`; the routing component is now a
        # `routingconnector`. We MUST emit it under `connectors:`.
        connectors = self.doc.get("connectors", {})
        self.assertIn(
            "routing",
            connectors,
            "missing connectors.routing — routing connector must live "
            "under connectors:, not processors: (the v0.106 bug we're "
            "fixing)",
        )

        processors = self.doc.get("processors", {})
        self.assertNotIn(
            "routing",
            processors,
            "routing must NOT live under processors: — OTel v0.106 "
            "removed the deprecated routingprocessor; emitting it under "
            "processors: would fail agent boot",
        )

    # 3. Routing connector wire shape — default + table ────────────────
    def test_routing_default_pipelines_is_raw_passthrough(self):
        routing = self.doc["connectors"]["routing"]
        self.assertEqual(
            routing.get("default_pipelines"),
            ["metrics/raw_passthrough"],
            "metrics not matched in the table must fall through to "
            "metrics/raw_passthrough",
        )
        self.assertIsInstance(
            routing.get("table", []),
            list,
            "routing.table must be a list of route() entries",
        )
        self.assertGreaterEqual(
            len(routing["table"]),
            5,
            "routing.table must dispatch to all 5 sketch families "
            "(plus any freshness-probe entries)",
        )
        # Each table entry must use the metric OTTL context — the
        # default resource context can't see metric.name, so the
        # connector would fail to build pipelines.
        for entry in routing["table"]:
            self.assertEqual(
                entry.get("context"),
                "metric",
                f"routing table entry must use context: metric "
                f"(otherwise OTTL evaluates against the resource "
                f"context and metric.name is unresolvable); got {entry!r}",
            )

    # 4. All 6 named pipelines (entry + 5 per-sketch + raw_passthrough) ─
    def test_all_named_pipelines_present(self):
        pipelines = self.doc["service"]["pipelines"]
        expected = [
            # Entry pipeline.
            "metrics",
            # Default raw-passthrough.
            "metrics/raw_passthrough",
        ] + SKETCH_PIPELINES
        # Total = 1 entry + 1 raw_passthrough + 5 per-family = 7.
        # The task spec phrases this as "6 named pipelines (entry + 5
        # per-sketch + raw_passthrough)" — i.e. ≥ 7 keys including the
        # entry. We assert each expected key is present.
        for pl in expected:
            self.assertIn(
                pl,
                pipelines,
                f"missing service.pipelines.{pl} — all per-family "
                f"pipelines must be pre-declared so a controller replan "
                f"is zero-touch on the pipeline graph",
            )

    # 5. Entry pipeline routes through the connector, not direct ───────
    def test_entry_pipeline_exports_to_routing_connector(self):
        entry = self.doc["service"]["pipelines"]["metrics"]
        self.assertEqual(
            entry.get("receivers"),
            ["otlp"],
            "entry pipeline receives from otlp",
        )
        self.assertEqual(
            entry.get("exporters"),
            ["routing"],
            "entry pipeline must export to the routing connector — "
            "fan-out is the connector's job, not the entry pipeline",
        )
        # No processors on entry pipeline.
        self.assertIn(
            entry.get("processors", []),
            ([], None),
            "entry pipeline must have no processors (connector fan-out)",
        )

    # 6. Each per-sketch pipeline has memory_limiter first, gorillas3 second ─
    def test_each_per_sketch_pipeline_has_memory_limiter_first(self):
        # Follow-up to PR #355's gorillas3 archive-write fix: even with
        # `window_interval: 5s` the agent was OOM-killed (exit 137) after
        # ~3 min under sustained load. memory_limiter MUST be the first
        # processor in every per-sketch pipeline so backpressure refuses
        # incoming batches BEFORE gorillas3 buffers them into the
        # in-memory windowState — limiting after gorillas3 would mean
        # the buffer has already accreted by the time the limiter
        # rejects.
        pipelines = self.doc["service"]["pipelines"]
        for pl_name, expected_family in zip(SKETCH_PIPELINES, SKETCH_PROCESSORS):
            pl = pipelines[pl_name]
            procs = pl.get("processors", [])
            self.assertGreater(
                len(procs),
                0,
                f"{pl_name} must have processors",
            )
            self.assertEqual(
                procs[0],
                "memory_limiter",
                f"{pl_name}: memory_limiter must run FIRST (apply "
                f"backpressure before gorillas3 buffers into "
                f"windowState — fixes agent OOM at ~3 min); got {procs!r}",
            )
            self.assertEqual(
                procs[1],
                "gorillas3",
                f"{pl_name}: gorillas3 must run SECOND (cold-tier write "
                f"on raw samples before sketch mutation); got {procs!r}",
            )
            self.assertIn(
                expected_family,
                procs,
                f"{pl_name}: expected sketch processor "
                f"{expected_family!r} to appear in the pipeline",
            )
            # batch should be last to coalesce per-pipeline output.
            self.assertEqual(
                procs[-1],
                "batch",
                f"{pl_name}: batch must be last so the gateway sees "
                f"properly framed OTLP; got {procs!r}",
            )
            self.assertEqual(
                pl.get("receivers"),
                ["routing"],
                f"{pl_name}: receivers must be the routing connector",
            )

    # 6b. memory_limiter processor block exists with chosen threshold ──
    def test_memory_limiter_processor_block_present(self):
        processors = self.doc.get("processors", {})
        self.assertIn(
            "memory_limiter",
            processors,
            "missing top-level memory_limiter processor — needed to "
            "apply backpressure before agent hits the 1.5 GiB cgroup "
            "ceiling and gets OOM-killed (exit 137)",
        )
        block = processors["memory_limiter"]
        # Threshold rationale: agent cgroup is 1536 MiB, so we pick
        # limit_mib: 1280 (≈ 80 % of cgroup) and spike_limit_mib: 256.
        self.assertEqual(
            block.get("limit_mib"),
            1280,
            "memory_limiter.limit_mib must be 1280 (≈ 80 % of agent's "
            "1536 MiB cgroup) — leaves 256 MiB headroom for short bursts",
        )
        self.assertEqual(
            block.get("spike_limit_mib"),
            256,
            "memory_limiter.spike_limit_mib must be 256 (mirrors gateway shape)",
        )
        self.assertEqual(
            block.get("check_interval"),
            "1s",
            "memory_limiter.check_interval must be 1s for tight feedback",
        )

    # 7. Raw passthrough pipeline shape ────────────────────────────────
    def test_raw_passthrough_pipeline_shape(self):
        raw = self.doc["service"]["pipelines"]["metrics/raw_passthrough"]
        self.assertEqual(raw.get("receivers"), ["routing"])
        # memory_limiter first, then gorillas3, then batch — no sketch
        # processor, so the metric name lands at the gateway verbatim.
        procs = raw.get("processors", [])
        self.assertEqual(
            procs,
            ["memory_limiter", "gorillas3", "batch"],
            "raw_passthrough must be [memory_limiter, gorillas3, batch] "
            "— memory_limiter first to apply backpressure before "
            "gorillas3 buffers into windowState; no sketch processor "
            "so the metric name lands at the gateway verbatim",
        )

    # 8. routing component is wired to NO sketch processor name ────────
    def test_routing_table_targets_only_real_pipelines(self):
        routing = self.doc["connectors"]["routing"]
        pipelines = set(self.doc["service"]["pipelines"].keys())
        for entry in routing["table"]:
            for tgt in entry.get("pipelines", []):
                self.assertIn(
                    tgt,
                    pipelines,
                    f"routing table targets pipeline {tgt!r} that is "
                    f"not declared in service.pipelines",
                )


if __name__ == "__main__":
    if not os.path.exists(PLACEHOLDER_PATH):
        print(f"FAIL: placeholder file not found at {PLACEHOLDER_PATH}", file=sys.stderr)
        sys.exit(2)
    unittest.main(verbosity=2)
