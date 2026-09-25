# Evidence Manifest

## Experiment

Temporal Range Completion Ownership Fence Performance Characterization

## Source revisions

Base:

    d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d

Patch:

    44ab5d807396872ed1cb440a217cee7b5f431089

Evidence branch:

    experiment/range-completion-perf-characterization

## Experimental scope

Backends:

- PostgreSQL 13.5
- Cassandra 5.0, single node, RF=1

Measurement design:

- same VM for base and patch
- GOMAXPROCS=2
- three paired A/B trials
- alternating base/patch execution order
- fresh populated range for every measured deletion
- setup, refill, validation, and teardown excluded from timing
- isolated cleanup and accelerated mixed-contention workloads

## Primary artifacts

- `report.md` — complete experiment report
- `summary.csv` — compact per-trial statistics
- `isolated-deltas.csv` — isolated cleanup deltas
- `commands-executed.md` — execution provenance
- `scripts/` — analysis and resource-monitoring code
- `common/persistence/tests/range_complete_perf_test.go` — benchmark harness

## Interpretation boundary

The experiment directly measures latency, throughput, overlap correlation,
errors, and resource state.

It does not directly measure server-internal Cassandra Paxos retries or establish
production RF>1 Cassandra latency.

The observed Cassandra contention-tail effect is therefore an empirical
observation associated with the patched conditional cleanup path; its exact
server-side mechanism remains subject to further investigation.
