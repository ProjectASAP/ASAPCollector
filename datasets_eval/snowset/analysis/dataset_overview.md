# Snowset Dataset Overview

This document compares the two Snowset parquet datasets available under `data/`. It is generated with [`overview.py`](overview.py).

---

## 1. Dataset Summary

| Property | `snowset-main` | `ts-explosion` (auxiliary) |
| --- | --- | --- |
| Role | Primary query-level records | Auxiliary time-series complement |
| Columns | 90 (index + 89 metrics) | 3 (index, `sec`, `queryId`) |
| Key column(s) | `queryId` (unique per row) | `queryId` (repeated), `sec` |
| Row granularity | One row per query execution | One row per second a query was active |
| Analysis target | Yes — all sketch analysis runs here | No — not analyzed independently |

> `ts-explosion` exists solely to provide `(timestamp, queryId)` pairs. Because it carries no query metrics, it does not need independent sketch analysis. It can be joined onto `snowset-main` on `queryId` when a time-series view of query activity is needed.

---

## 2. `snowset-main` — Column Schema

The main dataset records one row per completed query execution.

### Identity & Timing

| Column | Type | Description |
| --- | --- | --- |
| `queryId` | int64 | Unique query identifier (primary key) |
| `warehouseId` | int64 | Anonymized warehouse ID |
| `databaseId` | string | Anonymized database ID |
| `createdTime` | timestamp[us, UTC] | Query submission timestamp |
| `endTime` | timestamp[us, UTC] | Query completion timestamp |

### Duration Breakdown (ms)

| Column | Type | Description |
| --- | --- | --- |
| `durationTotal` | int64 | Wall-clock duration from submit to complete |
| `durationExec` | int64 | Execution-only duration |
| `durationControlPlane` | int64 | Control-plane overhead |
| `durationCompiling` | int64 | Compilation phase duration |
| `compilationTime` | int64 | Time spent in the compiler |
| `scheduleTime` | int64 | Time waiting to be scheduled |
| `execTime` | int64 | Actual execution time on servers |

### Resource Configuration

| Column | Type | Description |
| --- | --- | --- |
| `serverCount` | int64 | Number of servers used |
| `warehouseSize` | int64 | Warehouse size class |
| `perServerCores` | int64 | CPU cores per server |
| `userCpuTime` | int64 | User-space CPU time |
| `systemCpuTime` | int64 | Kernel CPU time |
| `memoryUsed` | int64 | Peak memory consumption (bytes) |

### Persistent Storage I/O (S3 / Cache)

| Column | Type |
| --- | --- |
| `persistentReadBytesS3` | int64 |
| `persistentReadRequestsS3` | int64 |
| `persistentReadBytesCache` | int64 |
| `persistentReadRequestsCache` | int64 |
| `persistentWriteBytesCache` | int64 |
| `persistentWriteRequestsCache` | int64 |
| `persistentWriteBytesS3` | int64 |
| `persistentWriteRequestsS3` | int64 |

### Intermediate Data I/O (Local SSD / S3 / Network)

| Column | Type |
| --- | --- |
| `intDataWriteBytesLocalSSD` | int64 |
| `intDataWriteRequestsLocalSSD` | int64 |
| `intDataReadBytesLocalSSD` | int64 |
| `intDataReadRequestsLocalSSD` | int64 |
| `intDataWriteBytesS3` | int64 |
| `intDataWriteRequestsS3` | int64 |
| `intDataReadBytesS3` | int64 |
| `intDataReadRequestsS3` | int64 |
| `intDataWriteBytesUncompressed` | int64 |
| `intDataNetReceivedBytes` | int64 |
| `intDataNetSentBytes` | int64 |
| `intDataNetSentRequests` | int64 |
| `intDataNetSentBytesUncompressed` | int64 |
| `ioRemoteExternalReadBytes` | int64 |
| `ioRemoteExternalReadRequests` | int64 |

### Scan & Result Cardinality

| Column | Type |
| --- | --- |
| `producedRows` | int64 |
| `returnedRows` | int64 |
| `scanAssignedBytes` | int64 |
| `scanAssignedFiles` | int64 |
| `scanBytes` | int64 |
| `scanFiles` | int64 |
| `scanOriginalFiles` | int64 |
| `filesCreated` | int64 |
| `fileStolenCount` | int64 |
| `remoteSeqScanFileOps` | int64 |
| `localSeqScanFileOps` | int64 |
| `localWriteFileOps` | int64 |
| `remoteSkipScanFileOps` | int64 |
| `remoteWriteFileOps` | int64 |

### Operator Profiling (`prof*`)

One int64 column per operator / subsystem. Values represent fractional time or cost attributed to that operator across the query.

| Column | Column | Column |
| --- | --- | --- |
| `profIdle` | `profCpu` | `profPersistentReadCache` |
| `profPersistentWriteCache` | `profPersistentReadS3` | `profPersistentWriteS3` |
| `profIntDataReadLocalSSD` | `profIntDataWriteLocalSSD` | `profIntDataReadS3` |
| `profIntDataWriteS3` | `profRemoteExtRead` | `profRemoteExtWrite` |
| `profResWriteS3` | `profFsMeta` | `profDataExchangeNet` |
| `profDataExchangeMsg` | `profControlPlaneMsg` | `profOs` |
| `profMutex` | `profSetup` | `profSetupMesh` |
| `profTeardown` | `profScanRso` | `profXtScanRso` |
| `profProjRso` | `profSortRso` | `profFilterRso` |
| `profResRso` | `profDmlRso` | `profHjRso` |
| `profBufRso` | `profFlatRso` | `profBloomRso` |
| `profAggRso` | `profBandRso` | `profPercentileRso` |
| `profUdtfRso` | | |

---

## 3. `ts-explosion` — Column Schema

The auxiliary dataset is intentionally minimal.

| Column | Type | Description |
| --- | --- | --- |
| `index` | int64 | Row index |
| `sec` | timestamp[us, UTC] | Second-level timestamp bucket |
| `queryId` | int64 | Foreign key into `snowset-main.queryId` |

Each query in `snowset-main` that spans *N* seconds produces *N* rows in `ts-explosion`, one per second. This enables time-windowed grouping of query activity without storing full metric columns in the time-series table.

---

## 4. Sample Rows (rows 1–3, via `overview.py`)

### `snowset-main`

```
queryId              warehouseId          databaseId           createdTime                      endTime                          durationTotal  durationExec  ...
282952223678799236   7891774171123969424  7097937327349925659  2018-03-02 14:44:02.768000+00:00  2018-03-02 14:44:40.431000+00:00  37663          2127         ...
6046452595346486634  7891774171123969424  7097937327349925659  2018-03-02 15:00:30.178000+00:00  2018-03-02 15:01:10.809000+00:00  40631          3631         ...
2644374019525554696  7891774171123969424  7097937327349925659  2018-03-02 13:14:02.842000+00:00  2018-03-02 13:14:37.807000+00:00  34965          3157         ...
```

### `ts-explosion`

```
sec                        queryId
2018-03-02 14:44:02+00:00  282952223678799236
2018-03-02 14:44:03+00:00  282952223678799236
2018-03-02 14:44:04+00:00  282952223678799236
```

The same `queryId` (`282952223678799236`) appears once in `snowset-main` and spans multiple consecutive seconds in `ts-explosion` — one row per second of its execution window.

---

## 5. Key Differences at a Glance

| Dimension | `snowset-main` | `ts-explosion` |
| --- | --- | --- |
| Columns | 90 | 3 |
| Row semantics | One query = one row | One second of activity = one row |
| `queryId` uniqueness | Unique (primary key) | Repeated (foreign key) |
| Carries metrics | Yes (latency, I/O, CPU, profiling, …) | No |
| Useful for sketching | Yes | No (use only as join key for timestamps) |
| Analyzed independently | Yes | No |
