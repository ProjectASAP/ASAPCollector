# ASAPCollector control-plane design

## TL;DR

ASAPPlanner analyzes the query workload and identifies suitable summary
semantics. The ASAPQuery-backend control plane chooses aggregation placement
and transmission mode, then issues the resulting collection plan.
ASAPCollector validates that it can execute the requested behavior, applies
the plan atomically, and reports evidence of the active plan. This document
defines only that ASAPCollector-specific contract.

**Status:** active

**MVP relationship:** in-scope. The MVP must prove that a planner-issued plan
was applied by the collector, not merely returned by the planning service.

## Scope and ownership

The control plane spans two distinct responsibilities:

| Responsibility | Owner |
| --- | --- |
| Analyze query workloads and accuracy SLAs | ASAPPlanner |
| Identify suitable summary families and parameters | ASAPPlanner |
| Choose aggregation placement and transmission mode | ASAPQuery-backend control plane |
| Assemble, version, and deliver the collection plan | ASAPQuery-backend control plane |
| Validate collector capabilities | ASAPCollector |
| Apply, expose, and acknowledge the active plan | ASAPCollector |

ASAPPlanner supplies query-to-summary planning information. The
ASAPQuery-backend control plane owns deployment decisions, including where
aggregation occurs and whether raw, full-summary, or delta-summary
transmission is selected. ASAPCollector must not reproduce either layer's
policy. The collector is an execution target with an explicit capability and
lifecycle contract.

## Collection plan

A plan addressed to ASAPCollector identifies:

- the metric or stream selection;
- the summary family and accuracy parameters;
- the label grouping and time-window placement;
- the transmission mode: raw/pass-through, full summary, or delta summary;
- the plan identity and version; and
- the activation and replacement conditions.

A plan must be explicit enough that an observer can determine what the
collector was instructed to compute and transmit. Defaults that change query
semantics must be resolved before the plan becomes active.

## Capability contract

ASAPCollector advertises the summary families, parameter ranges, aggregation
shapes, and transmission modes it supports. On receipt of a plan it must:

1. verify that the plan targets the intended collector and input stream;
2. validate every requested capability and parameter;
3. reject the whole plan if any required behavior is unsupported;
4. preserve the current valid plan when replacement fails; and
5. return an explicit accepted or rejected result tied to the plan identity.

Unsupported plans must never degrade silently into a different summary,
window, grouping, or transmission mode.

## Plan lifecycle

Plan application is atomic from the perspective of incoming metrics. A plan
has the following states:

- **received:** available for validation but not active;
- **rejected:** invalid or unsupported, with a reason;
- **active:** accepted and applied to new observations;
- **replaced:** superseded by a newer active plan; or
- **expired:** no longer valid under its declared lifetime.

The collector must define how existing window state is handled when a plan
changes. A change that alters summary semantics starts new compatible state;
state produced under incompatible plans must not be merged.

## Evidence of application

Acknowledging a plan is not sufficient proof that it affected collection.
ASAPCollector exposes evidence tied to the active plan identity:

- the accepted plan and resolved parameters;
- the time at which it became active;
- counters for observations processed under that plan;
- counters and bytes for each selected transmission mode;
- rejected observations or payloads; and
- the plan identity attached to emitted summary payloads.

The MVP harness captures this evidence and checks it against the plan issued
by the ASAPQuery-backend control plane.

## Failure behavior

- Invalid or unsupported plans are rejected explicitly.
- Loss of the control-plane connection does not invent a new plan.
- A configured last-known-good policy may continue only while it remains
  valid and its identity remains observable.
- Processing, encoding, and delivery failures are surfaced as failures rather
  than reported as successful plan application.
- Stale plans and acknowledgements from previous runs are not valid evidence.

## MVP acceptance contract

The MVP exercises raw/pass-through, full-summary, and supported delta-summary
decisions. For each decision it verifies that:

1. the ASAPQuery-backend control plane issued an explicit plan using the
   workload planning information supplied by ASAPPlanner;
2. ASAPCollector accepted and activated that exact plan;
3. observed metrics were processed under it;
4. emitted payloads match the selected mode; and
5. failures or unsupported behavior were reported explicitly.

Query-to-summary planning quality and deployment decision quality are outside
this document. They belong to ASAPPlanner and the ASAPQuery-backend control
plane respectively; this contract only requires an executable, observable
plan at the collector boundary.
