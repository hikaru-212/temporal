The benchmark-affecting commands below were executed. Read-only source inspections and tool-session polling are omitted. All generated harness/scripts are retained beside this file, so their exact contents are available.

1. Create detached worktrees and stop unused containers (from the real worktree):

```bash
mkdir -p /tmp/temporal-range-pilot-20260925/results /tmp/temporal-range-pilot-20260925/harness
git worktree add --detach /tmp/temporal-range-pilot-20260925/base d8f9c6d86b2cd0ea5c27d0694a598da84c6d337d
git worktree add --detach /tmp/temporal-range-pilot-20260925/patch 44ab5d807396872ed1cb440a217cee7b5f431089
docker stop temporal-dev-cassandra temporal-dev-mysql temporal-dev-grafana temporal-dev-elasticsearch temporal-dev-ui temporal-dev-prometheus
```

2. Write the temporary harness and scripts with shell here-documents, copy the same harness into both detached worktrees, format only the temporary harness, and compile:

```bash
cp /tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go /tmp/temporal-range-pilot-20260925/base/common/persistence/tests/range_complete_perf_test.go
cp /tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go /tmp/temporal-range-pilot-20260925/patch/common/persistence/tests/range_complete_perf_test.go
bash /tmp/temporal-range-pilot-20260925/compile.sh > /tmp/temporal-range-pilot-20260925/results/compile.log 2>&1
gofmt -w /tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go
```

The formatted harness was recopied to both worktrees. The repository lint command below initially reported one temporary-harness `confusing-results` naming issue. That was fixed by naming the return values of pilotSwap; the identical harness was recopied to both worktrees. Final commands:

```bash
bash /tmp/temporal-range-pilot-20260925/compile.sh > /tmp/temporal-range-pilot-20260925/results/compile-final.log 2>&1
cd /tmp/temporal-range-pilot-20260925/patch
GOMAXPROCS=2 make lint-code-fast GOLANGCI_LINT_BASE_REV=HEAD GOLANGCI_LINT_FIX=false GOLANGCI_LINT=/home/researcher/oss/worktrees/temporal-11869-timer-task-loss/.bin/golangci-lint-v2.13.0 ERRORTYPE=/home/researcher/oss/worktrees/temporal-11869-timer-task-loss/.bin/errortype > /tmp/temporal-range-pilot-20260925/results/lint-final.log 2>&1
```

compile.sh expanded for each revision:

```bash
cd /tmp/temporal-range-pilot-20260925/$revision
GOMAXPROCS=2 CGO_ENABLED=0 go test -mod=readonly -p=1 -tags=test_dep,integration,disable_grpc_modules -c -o /tmp/temporal-range-pilot-20260925/$revision.test ./common/persistence/tests
```

3. PostgreSQL smoke and initial excluded pass:

```bash
bash /tmp/temporal-range-pilot-20260925/smoke.sh PostgreSQL
bash /tmp/temporal-range-pilot-20260925/run_backend.sh PostgreSQL | tee /tmp/temporal-range-pilot-20260925/results/PostgreSQL-run-order.log
```

The first monitor polled docker stats. Its source was preserved as monitor-docker-original.py; monitor.py was replaced with direct cgroup/proc counters. Initial PostgreSQL trial artifacts and summaries were moved under results/excluded-initial-postgresql/. No initial-pass result is included in the final tables. A read-only `sudo -n true` check reported that a password was required; no sudo command changed host swap policy.

4. PostgreSQL replacement pass (included):

```bash
docker restart temporal-dev-postgresql
until docker exec temporal-dev-postgresql pg_isready -U temporal -d postgres; do sleep 1; done
bash /tmp/temporal-range-pilot-20260925/smoke.sh PostgreSQL
bash /tmp/temporal-range-pilot-20260925/run_backend.sh PostgreSQL | tee /tmp/temporal-range-pilot-20260925/results/PostgreSQL-run-order.log
```

5. Cassandra (only PostgreSQL/Cassandra were benchmarked; they never ran concurrently during measurement):

```bash
set -o pipefail
python3 /tmp/temporal-range-pilot-20260925/analyze.py > /tmp/temporal-range-pilot-20260925/results/analysis-postgresql.txt
docker stop temporal-dev-postgresql
docker start temporal-dev-cassandra
until docker exec temporal-dev-cassandra cqlsh -e 'DESCRIBE CLUSTER'; do sleep 2; done
bash /tmp/temporal-range-pilot-20260925/smoke.sh Cassandra
bash /tmp/temporal-range-pilot-20260925/run_backend.sh Cassandra | tee /tmp/temporal-range-pilot-20260925/results/Cassandra-run-order.log
docker stop temporal-dev-cassandra
```

6. Exact measured binary invocation inside run_backend.sh (each backend has trial 1 base→patch; trial 2 patch→base; trial 3 base→patch):

```bash
cd "$pilot_dir/$revision/common/persistence/tests"
env GOMAXPROCS=2 TEMPORAL_RANGE_PERF=1 RANGE_PERF_BACKEND="$backend" RANGE_PERF_REVISION="$revision" RANGE_PERF_TRIAL="$trial" RANGE_PERF_OUTPUT="$prefix.samples.jsonl" POSTGRES_SEEDS=127.0.0.1 POSTGRES_PORT=5432 CASSANDRA_SEEDS=127.0.0.1 CASSANDRA_PORT=9042 "$pilot_dir/$revision.test" -test.run='^TestRangeCompletePerf$' -test.parallel=1 -test.count=1 -test.timeout=30m -test.v > "$prefix.log" 2>&1
```

pilot_dir=/tmp/temporal-range-pilot-20260925; prefix=$pilot_dir/results/$backend-$revision-trial$trial. Smoke uses the same binary invocation with RANGE_PERF_SMOKE=1, trial 0, and a 10m timeout. See smoke.sh for the complete command.

Before and after each run, the runner executed date, free -h, df -h /, docker stats --no-stream, and docker inspect of the active backend for OOMKilled and StartedAt. Direct monitoring ran once per second in a separate Python process. The same monitor was used throughout the included PostgreSQL pass; Cassandra additionally recorded memory PSI.

7. Backend read-only configuration inspections:

```bash
docker exec temporal-dev-postgresql psql -U temporal -d postgres -Atc "SELECT version(), current_setting('shared_buffers'), current_setting('max_connections'), current_setting('synchronous_commit'), current_setting('fsync');"
docker exec temporal-dev-cassandra nodetool status
cat /proc/pressure/memory
```

8. Final analysis and verification:

```bash
python3 /tmp/temporal-range-pilot-20260925/analyze.py > /tmp/temporal-range-pilot-20260925/results/analysis-final.txt
python3 /tmp/temporal-range-pilot-20260925/report.py
sha256sum /tmp/temporal-range-pilot-20260925/harness/range_complete_perf_test.go /tmp/temporal-range-pilot-20260925/base/common/persistence/tests/range_complete_perf_test.go /tmp/temporal-range-pilot-20260925/patch/common/persistence/tests/range_complete_perf_test.go /tmp/temporal-range-pilot-20260925/base.test /tmp/temporal-range-pilot-20260925/patch.test
rg --files --hidden --no-ignore /tmp/temporal-range-pilot-20260925 > /tmp/temporal-range-pilot-20260925/results/file-inventory.txt
git -C /home/researcher/oss/worktrees/temporal-11869-timer-task-loss status -sb
```

The user clarified that small background host page-ins alone do not invalidate a screening run without container swap, swap-out, sustained pressure, or correlated pressure-related stalls. The final analysis retains those counters and applies that clarification; it does not discard latency outliers.
