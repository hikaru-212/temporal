# Range Completion Ownership Fence Performance Characterization

This directory preserves the methodology and compact evidence for the local A/B
performance characterization of the Temporal range-completion ownership fence.

The experiment was performed after correctness validation showed that fencing
`RangeCompleteHistoryTasks` by the decision-time shard `RangeID` closes the
stale-owner destructive-cleanup race.

The performance question was separate:

> What measurable cost does that ownership fence introduce?

## Main result

PostgreSQL showed additional cleanup latency and observable ownership-row
contention, without a material aggregate foreground throughput or p95
regression in this pilot.

Cassandra showed a repeatable cleanup/overlap tail-latency increase under an
intentionally accelerated same-shard contention workload, while aggregate
foreground throughput and overall foreground p95 remained stable.

The experiment does not establish the exact server-side Paxos mechanism behind
the Cassandra tail.

## Start here

- `report.md` — complete results and limitations
- `MANIFEST.md` — experiment identity and evidence contract
- `REPRODUCE.md` — reproduction requirements
- `summary.csv` — compact measurements
- `isolated-deltas.csv` — isolated cleanup comparison
- `commands-executed.md` — exact execution history

The full raw evidence is published separately as a GitHub Release asset.
