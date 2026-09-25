# Reproduction Guide

This characterization compares:

Base:

    d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d

Patch:

    44ab5d807396872ed1cb440a217cee7b5f431089

The runnable harness is:

    common/persistence/tests/range_complete_perf_test.go

The exact commands used for the recorded experiment are preserved in:

    commands-executed.md

## Experimental controls

For a meaningful A/B comparison:

1. Use the same machine for base and patch.
2. Use the same database version and configuration.
3. Compile both revisions before measurement.
4. Run one backend at a time.
5. Use a fresh populated range for every measured deletion.
6. Exclude fixture creation, refill, validation, and teardown from timing.
7. Alternate base/patch execution order across paired trials.
8. Record resource pressure and exclude trials affected by OOM or meaningful swapping.

## Important limitation

The Cassandra evidence was collected using a single-node RF=1 environment and
an accelerated cleanup cadence intended to increase collision probability.

It is a contention characterization, not a production Cassandra/Paxos
throughput benchmark.
