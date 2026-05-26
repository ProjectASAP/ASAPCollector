"""ASAP resource-usage cost model + AWS dollar-cost simulator.

A standalone evaluation simulator that models the per-component, per-resource
cost of the MVP-multinode arms (b0/b1/b2/b3/asap/asap-gzip) and prices them in
USD using a configurable AWS price book.

Modules
-------
coefficients : calibration constants (measured + assumed), each cited.
pricing      : AWS price book (EC2 / S3 / data-transfer rates), region pinned.
workloads    : Workload + ASAPConfig dataclasses and the MVP preset.
model        : per-component x per-resource resource formulas + USD mapping.
simulator    : CLI (arm + workload [+ asap config] -> resource + $ tables).
validate     : reproduces the FINDINGS wire-bandwidth numbers.

See README.md for run instructions and the measured-vs-assumed coefficient
table.
"""

__all__ = ["coefficients", "pricing", "workloads", "model", "simulator", "validate"]
