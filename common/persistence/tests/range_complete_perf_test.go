package tests

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/persistence/serialization"
	sqlstore "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/service/history/tasks"
)

type pilotObservationKey struct{}
type pilotObservation struct{ queries, batches, retries, errors atomic.Int64 }
type pilotObserver struct{}

func (pilotObserver) ObserveQuery(ctx context.Context, q gocql.ObservedQuery) {
	if o, ok := ctx.Value(pilotObservationKey{}).(*pilotObservation); ok {
		o.queries.Add(1)
		if q.Attempt > 0 {
			o.retries.Add(1)
		}
		if q.Err != nil {
			o.errors.Add(1)
		}
	}
}
func (pilotObserver) ObserveBatch(ctx context.Context, b gocql.ObservedBatch) {
	if o, ok := ctx.Value(pilotObservationKey{}).(*pilotObservation); ok {
		o.batches.Add(1)
		if b.Attempt > 0 {
			o.retries.Add(1)
		}
		if b.Err != nil {
			o.errors.Add(1)
		}
	}
}

type pilotSample struct {
	Revision     string `json:"revision"`
	Backend      string `json:"backend"`
	Trial        int    `json:"trial"`
	Scenario     string `json:"scenario"`
	Operation    string `json:"operation"`
	Rows         int    `json:"rows_expected"`
	Index        int    `json:"index"`
	Start        int64  `json:"start_unix_ns"`
	End          int64  `json:"end_unix_ns"`
	Duration     int64  `json:"duration_ns"`
	Error        string `json:"error,omitempty"`
	ErrorType    string `json:"error_type,omitempty"`
	Queries      int64  `json:"driver_queries"`
	Batches      int64  `json:"driver_batches"`
	Retries      int64  `json:"driver_retry_callbacks"`
	DriverErrors int64  `json:"driver_errors"`
	Overlap      bool   `json:"overlap"`
	LockWait     bool   `json:"lock_wait_observed"`
	Warmup       bool   `json:"warmup"`
	ActiveNS     int64  `json:"active_ns,omitempty"`
	SwapIn       int64  `json:"swap_in_pages,omitempty"`
	SwapOut      int64  `json:"swap_out_pages,omitempty"`
	Detail       string `json:"detail,omitempty"`
}
type pilot struct {
	t                 *testing.T
	em                p.ExecutionManager
	sm                p.ShardManager
	pg                *sql.DB
	out               *json.Encoder
	backend, revision string
	trial             int
	shard             int32
	rangeID           int64
	nextID            int64
	key               definition.WorkflowKey
	info              *persistencespb.WorkflowExecutionInfo
	state             *persistencespb.WorkflowExecutionState
	version           int64
}

func (h *pilot) emit(s pilotSample) {
	s.Backend = h.backend
	s.Revision = h.revision
	s.Trial = h.trial
	require.NoError(h.t, h.out.Encode(s))
}
func (h *pilot) call(scenario, operation string, rows, index int, fn func(context.Context) error) pilotSample {
	o := &pilotObservation{}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), pilotObservationKey{}, o), 5*time.Second)
	defer cancel()
	start := time.Now()
	err := fn(ctx)
	end := time.Now()
	s := pilotSample{Scenario: scenario, Operation: operation, Rows: rows, Index: index, Start: start.UnixNano(), End: end.UnixNano(), Duration: end.Sub(start).Nanoseconds(), Queries: o.queries.Load(), Batches: o.batches.Load(), Retries: o.retries.Load(), DriverErrors: o.errors.Load()}
	if err != nil {
		s.Error = err.Error()
		s.ErrorType = fmt.Sprintf("%T", err)
	}
	return s
}
func pilotSwap(t *testing.T) (in, out int64) {
	f, err := os.Open("/proc/vmstat")
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if fields[0] == "pswpin" || fields[0] == "pswpout" {
			v, e := strconv.ParseInt(fields[1], 10, 64)
			require.NoError(t, e)
			if fields[0] == "pswpin" {
				in = v
			} else {
				out = v
			}
		}
	}
	require.NoError(t, scan.Err())
	return in, out
}
func (h *pilot) phase(scenario string, rows, index int, start time.Time, in, out int64) {
	i, o := pilotSwap(h.t)
	h.emit(pilotSample{Scenario: scenario, Operation: "active_interval", Rows: rows, Index: index, Start: start.UnixNano(), End: time.Now().UnixNano(), ActiveNS: time.Since(start).Nanoseconds(), SwapIn: i - in, SwapOut: o - out})
}
func (h *pilot) newShard() {
	h.shard++
	h.nextID = 1
	h.rangeID = 2
	_, err := h.sm.GetOrCreateShard(context.Background(), &p.GetOrCreateShardRequest{ShardID: h.shard, InitialShardInfo: &persistencespb.ShardInfo{ShardId: h.shard, RangeId: h.rangeID, Owner: "range-pilot"}})
	require.NoError(h.t, err)
}
func (h *pilot) request(category tasks.Category, start, end int64) *p.RangeCompleteHistoryTasksRequest {
	req := &p.RangeCompleteHistoryTasksRequest{ShardID: h.shard, TaskCategory: category}
	if category == tasks.CategoryTimer {
		req.InclusiveMinTaskKey = tasks.NewKey(time.UnixMilli(1700000000000+start), 0)
		req.ExclusiveMaxTaskKey = tasks.NewKey(time.UnixMilli(1700000000000+end), 0)
	} else {
		req.InclusiveMinTaskKey = tasks.NewImmediateKey(start)
		req.ExclusiveMaxTaskKey = tasks.NewImmediateKey(end)
	}
	field := reflect.ValueOf(req).Elem().FieldByName("RangeID")
	require.Equal(h.t, h.revision == "patch", field.IsValid())
	if field.IsValid() {
		field.SetInt(h.rangeID)
	}
	return req
}
func (h *pilot) count(req *p.RangeCompleteHistoryTasksRequest) int {
	get := &p.GetHistoryTasksRequest{ShardID: req.ShardID, TaskCategory: req.TaskCategory, InclusiveMinTaskKey: req.InclusiveMinTaskKey, ExclusiveMaxTaskKey: req.ExclusiveMaxTaskKey, BatchSize: 1000}
	n := 0
	for {
		resp, err := h.em.GetHistoryTasks(context.Background(), get)
		require.NoError(h.t, err)
		n += len(resp.Tasks)
		get.NextPageToken = resp.NextPageToken
		if len(get.NextPageToken) == 0 {
			return n
		}
	}
}
func (h *pilot) populate(n int, category tasks.Category) *p.RangeCompleteHistoryTasksRequest {
	start := h.nextID
	h.nextID += int64(n) + 1
	req := h.request(category, start, start+int64(n))
	for offset := 0; offset < n; offset += 100 {
		batch := make([]tasks.Task, 0, 100)
		for j := offset; j < min(offset+100, n); j++ {
			id := start + int64(j)
			ts := time.UnixMilli(1700000000000 + id).UTC()
			if category == tasks.CategoryTimer {
				batch = append(batch, &tasks.UserTimerTask{WorkflowKey: h.key, TaskID: id, VisibilityTimestamp: ts, EventID: 5})
			} else {
				batch = append(batch, &tasks.ActivityTask{WorkflowKey: h.key, TaskID: id, VisibilityTimestamp: ts, ScheduledEventID: 5})
			}
		}
		require.NoError(h.t, h.em.AddHistoryTasks(context.Background(), &p.AddHistoryTasksRequest{ShardID: h.shard, RangeID: h.rangeID, NamespaceID: h.key.NamespaceID, WorkflowID: h.key.WorkflowID, ArchetypeID: chasm.WorkflowArchetypeID, Tasks: map[tasks.Category][]tasks.Task{category: batch}}))
	}
	require.Equal(h.t, n, h.count(req))
	return req
}
func (h *pilot) cleanup(scenario string, n, index int, req *p.RangeCompleteHistoryTasksRequest) pilotSample {
	return h.call(scenario, "cleanup", n, index, func(ctx context.Context) error { return h.em.RangeCompleteHistoryTasks(ctx, req) })
}
func (h *pilot) createWorkflow() {
	h.info = &persistencespb.WorkflowExecutionInfo{NamespaceId: h.key.NamespaceID, WorkflowId: h.key.WorkflowID, TaskQueue: "pilot", ExecutionStats: &persistencespb.ExecutionStats{}, VersionHistories: &historyspb.VersionHistories{Histories: []*historyspb.VersionHistory{{Items: []*historyspb.VersionHistoryItem{{EventId: 1, Version: 1}}}}}}
	h.state = &persistencespb.WorkflowExecutionState{RunId: h.key.RunID, CreateRequestId: "22222222-2222-4222-8222-222222222222", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING}
	h.version = 1
	_, err := h.em.CreateWorkflowExecution(context.Background(), &p.CreateWorkflowExecutionRequest{ShardID: h.shard, RangeID: h.rangeID, Mode: p.CreateWorkflowModeBrandNew, ArchetypeID: chasm.WorkflowArchetypeID, NewWorkflowSnapshot: p.WorkflowSnapshot{ExecutionInfo: h.info, ExecutionState: h.state, NextEventID: 2, DBRecordVersion: 1}})
	require.NoError(h.t, err)
}
func (h *pilot) write(scenario string, n, index int) pilotSample {
	mutation := p.WorkflowMutation{ExecutionInfo: h.info, ExecutionState: h.state, NextEventID: 2, DBRecordVersion: h.version + 1, Condition: 2}
	req := &p.UpdateWorkflowExecutionRequest{ShardID: h.shard, RangeID: h.rangeID, Mode: p.UpdateWorkflowModeUpdateCurrent, ArchetypeID: chasm.WorkflowArchetypeID, UpdateWorkflowMutation: mutation}
	s := h.call(scenario, "workflow_update", n, index, func(ctx context.Context) error { _, err := h.em.UpdateWorkflowExecution(ctx, req); return err })
	if s.Error == "" {
		h.version++
	}
	return s
}
func (h *pilot) validateWorkflow() {
	resp, err := h.em.GetWorkflowExecution(context.Background(), &p.GetWorkflowExecutionRequest{ShardID: h.shard, NamespaceID: h.key.NamespaceID, WorkflowID: h.key.WorkflowID, RunID: h.key.RunID, ArchetypeID: chasm.WorkflowArchetypeID})
	require.NoError(h.t, err)
	require.Equal(h.t, h.version, resp.DBRecordVersion)
	require.Empty(h.t, resp.State.ActivityInfos)
	require.Empty(h.t, resp.State.TimerInfos)
}
func (h *pilot) isolated(smoke bool) {
	h.newShard()
	for _, n := range []int{1000, 10000} {
		warm, count := 5, 20
		if n == 10000 {
			warm, count = 2, 10
		}
		if smoke {
			warm, count = 0, 1
		}
		for i := -warm; i < count; i++ {
			req := h.populate(n, tasks.CategoryTimer)
			in, out := pilotSwap(h.t)
			start := time.Now()
			s := h.cleanup("isolated", n, i, req)
			s.Warmup = i < 0
			h.phase("isolated", n, i, start, in, out)
			h.emit(s)
			require.Empty(h.t, s.Error)
			require.Zero(h.t, h.count(req))
		}
	}
}
func (h *pilot) mixed(smoke bool) {
	for _, n := range []int{1000, 10000} {
		h.newShard()
		h.createWorkflow()
		for i := 0; i < 50; i++ {
			s := h.write("warmup", n, i)
			require.Empty(h.t, s.Error)
		}
		segments, period := 60, time.Second
		if n == 10000 {
			segments, period = 12, 5*time.Second
		}
		if smoke {
			segments, period = 2, 200*time.Millisecond
		}
		total := time.Duration(0)
		for i := 0; i < segments; i++ {
			req := h.populate(n, tasks.CategoryTimer)
			writes := make(chan []pilotSample, 1)
			clean := make(chan pilotSample, 1)
			in, out := pilotSwap(h.t)
			start := time.Now()
			deadline := start.Add(period)
			go func() {
				samples := make([]pilotSample, 0, 10000)
				for time.Now().Before(deadline) {
					s := h.write("mixed", n, i)
					samples = append(samples, s)
					if s.Error != "" {
						break
					}
				}
				writes <- samples
			}()
			go func() {
				timer := time.NewTimer(100 * time.Millisecond)
				defer timer.Stop()
				<-timer.C
				clean <- h.cleanup("mixed", n, i, req)
			}()
			ws, cs := <-writes, <-clean
			elapsed := time.Since(start)
			total += elapsed
			h.phase("mixed", n, i, start, in, out)
			h.emit(cs)
			for _, s := range ws {
				s.Overlap = s.Start < cs.End && s.End > cs.Start
				h.emit(s)
			}
			require.Empty(h.t, cs.Error)
			require.Zero(h.t, h.count(req))
			h.validateWorkflow()
			for _, s := range ws {
				require.Empty(h.t, s.Error)
			}
		}
		h.emit(pilotSample{Scenario: "mixed", Operation: "case_total", Rows: n, ActiveNS: total.Nanoseconds()})
	}
}
func (h *pilot) contention(smoke bool) {
	h.newShard()
	target := 8
	if smoke {
		target = 2
	}
	overlaps := 0
	for i := 0; i < 40 && overlaps < target; i++ {
		req := h.populate(10000, tasks.CategoryTimer)
		in, out := pilotSwap(h.t)
		start := time.Now()
		ch := make(chan pilotSample, 1)
		go func() { ch <- h.cleanup("contention", 10000, i, req) }()
		var cs pilotSample
		pid := 0
		finished := false
		for pid == 0 && !finished {
			select {
			case cs = <-ch:
				finished = true
			default:
				err := h.pg.QueryRow("SELECT COALESCE((SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND query LIKE 'DELETE FROM timer_tasks%' LIMIT 1),0)").Scan(&pid)
				require.NoError(h.t, err)
			}
		}
		if finished {
			h.phase("contention_missed", 10000, i, start, in, out)
			cs.Scenario = "contention_missed"
			h.emit(cs)
			require.Empty(h.t, cs.Error)
			require.Zero(h.t, h.count(req))
			continue
		}
		done := make(chan pilotSample, 1)
		go func() {
			done <- h.call("contention", "shard_update", 10000, i, func(ctx context.Context) error {
				return h.sm.UpdateShard(ctx, &p.UpdateShardRequest{ShardInfo: &persistencespb.ShardInfo{ShardId: h.shard, RangeId: h.rangeID, Owner: "range-pilot"}, PreviousRangeID: h.rangeID})
			})
		}()
		waited := false
		var us pilotSample
		updating := true
		for updating {
			select {
			case us = <-done:
				updating = false
			default:
				var blocked bool
				err := h.pg.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))", pid).Scan(&blocked)
				require.NoError(h.t, err)
				waited = waited || blocked
			}
		}
		cs = <-ch
		overlap := us.Start < cs.End && us.End > cs.Start
		cs.Overlap = overlap
		us.Overlap = overlap
		us.LockWait = waited
		if overlap {
			overlaps++
		} else {
			cs.Scenario = "contention_missed"
			us.Scenario = "contention_missed"
		}
		h.phase(cs.Scenario, 10000, i, start, in, out)
		h.emit(cs)
		h.emit(us)
		require.Empty(h.t, cs.Error)
		require.Empty(h.t, us.Error)
		require.Zero(h.t, h.count(req))
	}
	require.GreaterOrEqual(h.t, overlaps, target)
}
func TestRangeCompletePerf(t *testing.T) {
	if os.Getenv("TEMPORAL_RANGE_PERF") != "1" {
		t.Skip("opt-in persistence performance pilot")
	}
	h := &pilot{t: t, backend: os.Getenv("RANGE_PERF_BACKEND"), revision: os.Getenv("RANGE_PERF_REVISION"), key: definition.NewWorkflowKey("11111111-1111-4111-8111-111111111111", "range-pilot", "33333333-3333-4333-8333-333333333333")}
	var err error
	h.trial, err = strconv.Atoi(os.Getenv("RANGE_PERF_TRIAL"))
	require.NoError(t, err)
	f, err := os.Create(os.Getenv("RANGE_PERF_OUTPUT"))
	require.NoError(t, err)
	buffer := bufio.NewWriterSize(f, 256*1024)
	h.out = json.NewEncoder(buffer)
	defer func() { require.NoError(t, buffer.Flush()); require.NoError(t, f.Close()) }()
	logger := log.NewNoopLogger()
	serializer := serialization.NewSerializer()
	var factory p.DataStoreFactory
	switch h.backend {
	case "PostgreSQL":
		cfg := NewPostgreSQLConfig("postgres12", nil)
		cfg.MaxConns = 16
		cfg.MaxIdleConns = 16
		SetupPostgreSQLDatabase(t, cfg)
		SetupPostgreSQLSchema(t, cfg)
		defer TearDownPostgreSQLDatabase(t, cfg)
		factory = sqlstore.NewFactory(*cfg, resolver.NewNoopResolver(), testPostgreSQLClusterName, logger, metrics.NoopMetricsHandler, serializer)
		h.pg, err = sql.Open("postgres", fmt.Sprintf("host=127.0.0.1 port=5432 user=temporal password=temporal dbname=%s sslmode=disable application_name=range_pilot_observer", cfg.DatabaseName))
		require.NoError(t, err)
		h.pg.SetMaxOpenConns(1)
		defer func() { require.NoError(t, h.pg.Close()) }()
		h.emit(pilotSample{Operation: "configuration", Detail: fmt.Sprintf("database=%s plugin=postgres12 MaxConns=16 MaxIdleConns=16 observer_pool=1", cfg.DatabaseName)})
	case "Cassandra":
		cfg := NewCassandraConfig()
		cfg.MaxConns = 2
		SetUpCassandraDatabase(t, cfg, logger)
		SetUpCassandraSchema(t, cfg, logger)
		defer TearDownCassandraKeyspace(t, cfg)
		session, e := commongocql.NewSession(func() (*gocql.ClusterConfig, error) {
			cluster, e := commongocql.NewCassandraCluster(*cfg, resolver.NewNoopResolver())
			if e == nil {
				cluster.QueryObserver = pilotObserver{}
				cluster.BatchObserver = pilotObserver{}
			}
			return cluster, e
		}, logger, metrics.NoopMetricsHandler)
		require.NoError(t, e)
		factory = cassandra.NewFactoryFromSession(*cfg, testCassandraClusterName, logger, session, serializer)
		h.emit(pilotSample{Operation: "configuration", Detail: fmt.Sprintf("keyspace=%s RF=1 MaxConns=2 consistency=LOCAL_QUORUM serial=LOCAL_SERIAL", cfg.Keyspace)})
	default:
		t.Fatalf("unknown backend %q", h.backend)
	}
	defer factory.Close()
	ss, err := factory.NewShardStore()
	require.NoError(t, err)
	es, err := factory.NewExecutionStore()
	require.NoError(t, err)
	h.sm = p.NewShardManager(ss, serializer)
	h.em = p.NewExecutionManager(es, serializer, nil, logger, dynamicconfig.GetIntPropertyFn(4*1024*1024), dynamicconfig.GetBoolPropertyFn(false))
	defer h.sm.Close()
	defer h.em.Close()
	smoke := os.Getenv("RANGE_PERF_SMOKE") == "1"
	h.newShard()
	req := h.populate(10, tasks.CategoryTransfer)
	s := h.cleanup("smoke_transfer", 10, 0, req)
	h.emit(s)
	require.Empty(t, s.Error)
	require.Zero(t, h.count(req))
	h.isolated(smoke)
	h.mixed(smoke)
	if h.backend == "PostgreSQL" {
		h.contention(smoke)
	}
}
