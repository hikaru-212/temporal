//! Investigation-only harness. The same module is compiled against each unmodified production
//! revision; timing ends before the independent live-history cold replay begins.

use crate::{
    PollError, WorkflowTaskCompletion, init_replay_worker,
    replay::{HistoryForReplay, ReplayWorkerInput, TestHistoryBuilder},
    test_help::{
        MocksHolder, ResponseType, WorkerExt, hist_to_poll_resp, mock_worker, query_ok,
        schedule_activity_cmd, schedule_local_activity_cmd, start_timer_cmd, test_worker_cfg,
    },
    worker::{LEGACY_QUERY_ID, client::mocks::mock_worker_client},
};
use futures_util::{StreamExt, stream};
use prost::Message as _;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::Write as _,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use temporalio_common::protos::{
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        common::NamespacedWorkflowExecution,
        workflow_activation::{
            WorkflowActivation, remove_from_cache::EvictionReason,
            workflow_activation_job::Variant as Job,
        },
        workflow_commands::{
            ActivityCancellationType, ModifyWorkflowProperties, RequestCancelActivity,
            SignalExternalWorkflowExecution, signal_external_workflow_execution, workflow_command,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        command::v1::{Command, command::Attributes as CmdAttrs},
        common::v1::{Memo, Payload},
        enums::v1::{CommandType, EventType},
        history::v1::{
            ActivityTaskCanceledEventAttributes, History, MarkerRecordedEventAttributes,
            TimerStartedEventAttributes, WorkflowPropertiesModifiedEventAttributes, history_event,
        },
        protocol::v1::Message as ProtocolMessage,
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, PollWorkflowTaskQueueResponse,
            RespondQueryTaskCompletedResponse,
        },
    },
};
use tokio::sync::{Notify, mpsc};

const WFID: &str = "final-benchmark-1766";
const UPDATE_ID: &str = "unused";

#[derive(Default)]
struct Obs {
    query_start: Option<Instant>,
    query_done: Option<Instant>,
    next_start: Option<Instant>,
    next_done: Option<Instant>,
    completions: Vec<WorkflowTaskCompletion>,
    evictions: usize,
    fetches: usize,
    la_executions: usize,
    query_answer: String,
    replay_activations: usize,
}

fn stable_history(h: &mut History) {
    for e in &mut h.events {
        e.event_time = Some(prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        });
        if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(a)) =
            &mut e.attributes
        {
            a.original_execution_run_id = "00000000-0000-4000-8000-000000000001".to_owned();
        }
        if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(a)) =
            &mut e.attributes
        {
            if let Some(m) = &mut a.sdk_metadata {
                m.core_used_flags.sort_unstable();
                m.lang_used_flags.sort_unstable();
            }
        }
    }
}

fn history_of(t: &TestHistoryBuilder) -> History {
    let mut h: History = t.get_full_history_info().unwrap().into();
    stable_history(&mut h);
    h
}

fn poll(t: &TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    let mut p = hist_to_poll_resp(t, WFID, ResponseType::AllHistory).resp;
    // Fixed run IDs make serialized inputs comparable between independent processes.
    p.workflow_execution.as_mut().unwrap().run_id = "benchmark-run".to_owned();
    stable_history(p.history.as_mut().unwrap());
    p
}

fn incremental(t: &TestHistoryBuilder) -> PollWorkflowTaskQueueResponse {
    let n = history_of(t)
        .events
        .iter()
        .filter(|e| e.event_type() == EventType::WorkflowTaskStarted)
        .count();
    let mut p = hist_to_poll_resp(t, WFID, ResponseType::OneTask(n)).resp;
    p.workflow_execution.as_mut().unwrap().run_id = "benchmark-run".to_owned();
    stable_history(p.history.as_mut().unwrap());
    p
}

fn legacy_query(t: &TestHistoryBuilder, advertised: i64) -> PollWorkflowTaskQueueResponse {
    let mut p = poll(t);
    p.query = Some(WorkflowQuery {
        query_type: "state".to_owned(),
        ..Default::default()
    });
    p.started_event_id = 0;
    p.previous_started_event_id = advertised;
    p
}

fn prefix(cycles: u32, held: bool) -> TestHistoryBuilder {
    let mut t = TestHistoryBuilder::default();
    if held {
        t.add_wfe_started_with_wft_timeout(Duration::from_secs(1));
    } else {
        t.add_by_type(EventType::WorkflowExecutionStarted);
    }
    t.add_workflow_task_scheduled_and_started();
    for seq in 1..=cycles {
        t.add_workflow_task_completed();
        let started = t.add_timer_started(seq.to_string());
        t.add_timer_fired(started, seq.to_string());
        t.add_workflow_task_scheduled_and_started();
    }
    t
}

// Save the exact stable event bytes used by mock polls and fetches so the two builds can be
// checked for identical workload inputs independently of their latency measurements.
fn fixture(sc: &str, cycles: u32, t: &TestHistoryBuilder, tasks: &[PollWorkflowTaskQueueResponse]) {
    if let Ok(dir) = std::env::var("B1766_FIXTURES") {
        let h = history_of(t);
        let path = format!("{dir}/{sc}-{cycles}.history.bin");
        std::fs::write(path, h.encode_to_vec()).unwrap();
        let boundaries = tasks.iter().map(|p| json!({
            "query": p.query.is_some(), "previous_started": p.previous_started_event_id,
            "started": p.started_event_id, "event_ids": p.history.as_ref().unwrap().events.iter().map(|e| e.event_id).collect::<Vec<_>>()
        })).collect::<Vec<_>>();
        std::fs::write(
            format!("{dir}/{sc}-{cycles}.tasks.json"),
            serde_json::to_vec_pretty(&boundaries).unwrap(),
        )
        .unwrap();
    }
}

fn metrics(o: &Obs, t: &TestHistoryBuilder) -> Value {
    let ms = |a: Instant, b: Instant| (b - a).as_secs_f64() * 1000.0;
    let next_done = o.next_done.unwrap();
    let query = o.query_start.zip(o.query_done).map(|(a, b)| ms(a, b));
    // Keep the query-response-to-WFT-completion definition from the V4.0 timing experiment.
    // Also expose poll-to-completion so handoff time cannot be mistaken for Core work.
    let next = ms(o.query_done.unwrap_or(o.next_start.unwrap()), next_done);
    let h = history_of(t);
    json!({"hist_events":h.events.len(), "hist_bytes": h.encoded_len(),
        "wft_count":h.events.iter().filter(|e| e.event_type()==EventType::WorkflowTaskStarted).count(),
        "query_ms":query,"next_wft_ms":next,
        "next_poll_ms":ms(o.next_start.unwrap(),next_done),
        "combined_ms":ms(o.query_start.unwrap_or(o.next_start.unwrap()),next_done),
        "evictions":o.evictions,"fetches":o.fetches,"la_executions":o.la_executions,
        "la_reexecutions":o.la_executions.saturating_sub(1),
        "query_answer":o.query_answer,"rebuild_replay_activations":o.replay_activations,
        "live_commands":cmd_types(&o.completions.last().unwrap().commands)})
}

#[derive(Default, PartialEq, Debug)]
struct State {
    acked: bool,
    cancelled: bool,
}

fn core_lang(
    sc: &str,
    cycles: u32,
    a: &WorkflowActivation,
    state: &mut State,
) -> Vec<workflow_command::Variant> {
    let mut cmds = vec![];
    for j in a.jobs.iter().filter_map(|j| j.variant.as_ref()) {
        match j {
            Job::InitializeWorkflow(_) => {
                *state = State::default();
                cmds.push(start_timer_cmd(1, Duration::from_secs(1)));
            }
            Job::FireTimer(ft) if ft.seq < cycles => {
                cmds.push(start_timer_cmd(ft.seq + 1, Duration::from_secs(1)))
            }
            Job::FireTimer(ft) if ft.seq == cycles => {
                cmds.push(if sc == "cancel" {
                    schedule_activity_cmd(
                        1,
                        "q",
                        "1",
                        ActivityCancellationType::WaitCancellationCompleted,
                        Duration::from_secs(60),
                        Duration::from_secs(60),
                    )
                } else {
                    signal_external()
                });
            }
            Job::ResolveSignalExternalWorkflow(_) => {
                state.acked = true;
                cmds.push(memo());
            }
            Job::SignalWorkflow(_) if sc == "cancel" => {
                cmds.push(RequestCancelActivity { seq: 1 }.into());
                cmds.push(start_timer_cmd(cycles + 1, Duration::from_secs(1)));
            }
            Job::ResolveActivity(_) if sc == "cancel" => {
                state.cancelled = true;
                cmds.push(if a.history_length < 5 * cycles + 14 {
                    memo()
                } else {
                    start_timer_cmd(cycles + 2, Duration::from_secs(1))
                });
            }
            other => panic!("unexpected core job {other:?}"),
        }
    }
    cmds
}

async fn run_core(sc: &str, cycles: u32, verify: bool) -> Value {
    let mut t = prefix(cycles, false);
    let mut tasks = vec![poll(&t)];
    let mut advertised = t.current_event_id();
    t.add_workflow_task_completed();
    if sc == "cancel" {
        let scheduled = t.add_activity_task_scheduled("1");
        t.add_we_signaled("S", vec![]);
        t.add_workflow_task_scheduled_and_started();
        tasks.push(incremental(&t));
        advertised = t.current_event_id();
        t.add_workflow_task_completed();
        t.add_activity_task_cancel_requested(scheduled);
        let requested = t.current_event_id();
        t.add(ActivityTaskCanceledEventAttributes {
            scheduled_event_id: scheduled,
            latest_cancel_requested_event_id: requested,
            ..Default::default()
        });
        t.add_timer_started((cycles + 1).to_string());
        t.add_workflow_task_scheduled();
        tasks.push(legacy_query(&t, advertised));
    } else {
        let initiated = t.add_signal_wf("wake", "signal-target", "");
        if sc == "non_effectful" {
            tasks.push(legacy_query(&t, advertised));
        }
        t.add_external_signal_completed(initiated);
        t.add_workflow_task_scheduled();
        if sc == "trailing" {
            tasks.push(legacy_query(&t, advertised));
        }
    }
    t.add_workflow_task_started();
    tasks.push(incremental(&t));
    if verify {
        fixture(sc, cycles, &t, &tasks);
    }
    let n = tasks.len();
    let expected_completions = if sc == "cancel" { 3 } else { 2 };
    let o = Arc::new(Mutex::new(Obs::default()));
    let answered = Arc::new(Notify::new());
    let done = Arc::new(Notify::new());
    let mut client = mock_worker_client();
    {
        let (o, answered, done) = (o.clone(), answered.clone(), done.clone());
        client
            .expect_complete_workflow_task()
            .returning(move |c, _| {
                let mut o = o.lock().unwrap();
                if o.completions.len() + 1 == expected_completions {
                    o.next_done = Some(Instant::now());
                    done.notify_one();
                }
                o.completions.push(c);
                answered.notify_one();
                Ok(Default::default())
            });
    }
    {
        let (o, answered) = (o.clone(), answered.clone());
        client.expect_respond_legacy_query().returning(move |_, _| {
            o.lock().unwrap().query_done = Some(Instant::now());
            answered.notify_one();
            Ok(RespondQueryTaskCompletedResponse::default())
        });
    }
    {
        let o = o.clone();
        let h = history_of(&t);
        client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                o.lock().unwrap().fetches += 1;
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(h.clone()),
                    ..Default::default()
                })
            });
    }
    let task_stream = {
        let o = o.clone();
        let answered = answered.clone();
        stream::iter(tasks.into_iter().enumerate())
            .then(move |(i, p)| {
                let o = o.clone();
                let answered = answered.clone();
                async move {
                    if i > 0 {
                        answered.notified().await;
                    }
                    if p.query.is_some() {
                        o.lock().unwrap().query_start = Some(Instant::now());
                    }
                    if i == n - 1 {
                        o.lock().unwrap().next_start = Some(Instant::now());
                    }
                    p
                }
            })
            .chain(stream::pending())
    };
    let mut mock = MocksHolder::from_wft_stream(client, task_stream);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let mut state = State::default();
    loop {
        let a = tokio::select! {biased; _=done.notified()=>break, a=core.poll_workflow_activation()=>a.unwrap()};
        let cmds = if a.eviction_reason().is_some() {
            assert_ne!(a.eviction_reason(), Some(EvictionReason::Nondeterminism));
            o.lock().unwrap().evictions += 1;
            state = State::default();
            vec![]
        } else if let Some(Job::QueryWorkflow(q)) = a.jobs.first().and_then(|j| j.variant.as_ref())
        {
            let answer = format!("acked={} cancelled={}", state.acked, state.cancelled);
            o.lock().unwrap().query_answer = answer.clone();
            vec![query_ok(q.query_id.clone(), &answer)]
        } else {
            if a.is_replaying && o.lock().unwrap().next_start.is_some() {
                o.lock().unwrap().replay_activations += 1;
            }
            core_lang(sc, cycles, &a, &mut state)
        };
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(a.run_id, cmds))
            .await
            .unwrap();
    }
    core.drain_pollers_and_shutdown().await;
    let o = std::mem::take(&mut *o.lock().unwrap());
    let mut row = metrics(&o, &t);
    assert_eq!(o.completions.len(), expected_completions);
    assert_eq!(o.query_done.is_some(), sc != "normal");
    assert_eq!(
        row["live_commands"],
        if sc == "cancel" {
            "START_TIMER"
        } else {
            "MODIFY_WORKFLOW_PROPERTIES"
        }
    );
    if verify {
        let c = o.completions.last().unwrap();
        commit(&mut t, &c.commands, &c.messages);
        let mut replay_state = State::default();
        let (verdict, acts) = cold_replay(t, |a| core_lang(sc, cycles, a, &mut replay_state)).await;
        assert_eq!(verdict, "ok");
        assert_eq!(state, replay_state);
        row["cold_replay"] = json!(verdict);
        row["cold_replay_activations"] = json!(acts);
        row["state_equivalent"] = json!(true);
    }
    row
}

#[derive(Default)]
struct HeldState {
    cycles: u32,
    acked: bool,
    la_done: bool,
    decision: Option<bool>,
}
impl HeldState {
    fn activate(&mut self, a: &WorkflowActivation) -> Vec<workflow_command::Variant> {
        let mut cmds = vec![];
        for j in a.jobs.iter().filter_map(|j| j.variant.as_ref()) {
            match j {
                Job::InitializeWorkflow(_) => cmds.push(start_timer_cmd(1, Duration::from_secs(1))),
                Job::FireTimer(f) if f.seq <= self.cycles => {
                    cmds.push(start_timer_cmd(f.seq + 1, Duration::from_secs(1)))
                }
                Job::FireTimer(_) => {
                    cmds.push(schedule_local_activity_cmd(
                        1,
                        "1",
                        ActivityCancellationType::TryCancel,
                        Duration::from_secs(60),
                    ));
                    cmds.push(signal_external());
                }
                Job::ResolveSignalExternalWorkflow(_) => self.acked = true,
                Job::ResolveActivity(_) => self.la_done = true,
                other => panic!("unexpected held-LA job {other:?}"),
            }
        }
        if self.acked && self.decision.is_none() {
            self.decision = Some(self.la_done);
            cmds.push(if self.la_done {
                start_timer_cmd(self.cycles + 2, Duration::from_secs(10))
            } else {
                memo()
            });
        }
        cmds
    }
}

async fn run_held(cycles: u32, verify: bool) -> Value {
    let mut history = prefix(cycles, true);
    let (tasks, next_tasks) = mpsc::unbounded_channel();
    let initial = poll(&history);
    tasks.send(initial.clone()).unwrap();
    let o = Arc::new(Mutex::new(Obs::default()));
    let current_history = Arc::new(Mutex::new(history_of(&history)));
    let (completion_tx, mut completions) = mpsc::unbounded_channel();
    let answered = Arc::new(Notify::new());
    let mut client = mock_worker_client();
    {
        let o = o.clone();
        client
            .expect_complete_workflow_task()
            .returning(move |c, _| {
                let mut o = o.lock().unwrap();
                if o.next_start.is_some() && has_cmd(&c, CommandType::RecordMarker) {
                    o.next_done = Some(Instant::now());
                }
                o.completions.push(c.clone());
                completion_tx.send(c).unwrap();
                Ok(Default::default())
            });
    }
    {
        let o = o.clone();
        let answered = answered.clone();
        client.expect_respond_legacy_query().returning(move |_, _| {
            o.lock().unwrap().query_done = Some(Instant::now());
            answered.notify_one();
            Ok(Default::default())
        });
    }
    {
        let o = o.clone();
        let h = current_history.clone();
        client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                o.lock().unwrap().fetches += 1;
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(h.lock().unwrap().clone()),
                    ..Default::default()
                })
            });
    }
    let task_stream = {
        let o = o.clone();
        tokio_stream::wrappers::UnboundedReceiverStream::new(next_tasks).map(move |p| {
            if p.query.is_some() {
                o.lock().unwrap().query_start = Some(Instant::now());
            } else if o.lock().unwrap().query_done.is_some() {
                o.lock()
                    .unwrap()
                    .next_start
                    .get_or_insert_with(Instant::now);
            }
            p
        })
    };
    let mut mock = MocksHolder::from_wft_stream(client, task_stream);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);
    let mut wf = HeldState {
        cycles,
        ..Default::default()
    };
    let first = loop {
        tokio::select! {biased;
            c=completions.recv()=>break c.unwrap(),
            a=core.poll_workflow_activation()=>{let a=a.unwrap();let cmds=wf.activate(&a);
                core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(a.run_id,cmds)).await.unwrap();}
        }
    };
    record_completion(&mut history, &first);
    history.add_timer_fired(history.current_event_id(), (cycles + 1).to_string());
    history.add_workflow_task_scheduled_and_started();
    let mut with_query = poll(&history);
    with_query.queries.insert(
        "q".to_owned(),
        WorkflowQuery {
            query_type: "attached".to_owned(),
            ..Default::default()
        },
    );
    tasks.send(with_query.clone()).unwrap();
    let a = core.poll_workflow_activation().await.unwrap();
    let cmds = wf.activate(&a);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(a.run_id, cmds))
        .await
        .unwrap();
    let q = core.poll_workflow_activation().await.unwrap();
    assert_eq!(query_ids(&q), ["q"]);
    let la = core.poll_activity_task().await.unwrap();
    o.lock().unwrap().la_executions += 1;
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: la.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        q.run_id,
        query_ok("q", "answer"),
    ))
    .await
    .unwrap();
    let c = completions.recv().await.unwrap();
    assert!(c.force_create_new_workflow_task);
    assert_eq!(cmd_types(&c.commands), "SIGNAL_EXTERNAL_WORKFLOW_EXECUTION");
    let advertised = history.current_event_id();
    record_completion(&mut history, &c);
    let initiated = history.current_event_id();
    history.add_workflow_task_scheduled();
    history.add_external_signal_completed(initiated);
    let query = legacy_query(&history, advertised);
    history.add_workflow_task_started();
    let next = incremental(&history);
    *current_history.lock().unwrap() = history_of(&history);
    if verify {
        fixture(
            "held_la",
            cycles,
            &history,
            &[initial, with_query, query.clone(), next.clone()],
        );
    }
    let measured_history = history.clone();
    tasks.send(query).unwrap();
    loop {
        let a = core.poll_workflow_activation().await.unwrap();
        let cmds = if query_ids(&a) == [LEGACY_QUERY_ID] {
            let answer = format!("acked={} la_done={}", wf.acked, wf.la_done);
            o.lock().unwrap().query_answer = answer.clone();
            vec![query_ok(LEGACY_QUERY_ID, &answer)]
        } else {
            wf.activate(&a)
        };
        let is_query = !query_ids(&a).is_empty();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(a.run_id, cmds))
            .await
            .unwrap();
        if is_query {
            break;
        }
    }
    answered.notified().await;
    tasks.send(next).unwrap();
    let local_activities = async {
        loop {
            let task = core.poll_activity_task().await.unwrap();
            o.lock().unwrap().la_executions += 1;
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        }
    };
    let workflow_tasks = async {
        loop {
            tokio::select! {biased;
                c=completions.recv()=>{let c=c.unwrap();record_completion(&mut history,&c);
                    if has_cmd(&c,CommandType::RecordMarker){break;}
                    assert!(c.force_create_new_workflow_task);
                    history.add_workflow_task_scheduled_and_started();*current_history.lock().unwrap()=history_of(&history);
                    tasks.send(incremental(&history)).unwrap();}
                a=core.poll_workflow_activation()=>{let a=a.unwrap();
                    let cmds=if a.eviction_reason().is_some(){assert_ne!(a.eviction_reason(),Some(EvictionReason::Nondeterminism));
                        o.lock().unwrap().evictions+=1;wf=HeldState{cycles,..Default::default()};vec![]}
                    else {if a.is_replaying{o.lock().unwrap().replay_activations+=1;}wf.activate(&a)};
                    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(a.run_id,cmds)).await.unwrap();}
            }
        }
    };
    tokio::select! {_=local_activities=>unreachable!(),_=workflow_tasks=>{}}
    core.drain_pollers_and_shutdown().await;
    let o = std::mem::take(&mut *o.lock().unwrap());
    let mut row = metrics(&o, &measured_history);
    assert_eq!(
        history_of(&history)
            .events
            .iter()
            .filter(|e| e.event_type() == EventType::MarkerRecorded)
            .count(),
        1
    );
    row["live_decision"] = json!(wf.decision);
    if verify {
        let mut replay_wf = HeldState {
            cycles,
            ..Default::default()
        };
        let (verdict, acts) = cold_replay(history, |a| replay_wf.activate(a)).await;
        assert_eq!(verdict, "ok");
        assert_eq!(wf.decision, replay_wf.decision);
        assert_eq!((wf.acked, wf.la_done), (replay_wf.acked, replay_wf.la_done));
        row["cold_replay"] = json!(verdict);
        row["cold_replay_activations"] = json!(acts);
        row["state_equivalent"] = json!(true);
    }
    row
}

#[tokio::test]
async fn final_benchmark() {
    let Ok(sc) = std::env::var("B1766_SCENARIO") else {
        return;
    };
    let num = |k: &str, d: usize| {
        std::env::var(k)
            .ok()
            .map(|v| v.parse().unwrap())
            .unwrap_or(d)
    };
    let cycles = num("B1766_CYCLES", 18) as u32;
    let samples = num("B1766_SAMPLES", 20);
    let warmup = num("B1766_WARMUP", 5);
    let verify = num("B1766_VERIFY", 0) == 1;
    let label = std::env::var("B1766_LABEL").unwrap();
    let round = num("B1766_ROUND", 0);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(std::env::var("B1766_OUT").unwrap())
        .unwrap();
    for i in 0..samples + warmup {
        let fut = async {
            if sc == "held_la" {
                run_held(cycles, verify).await
            } else {
                run_core(&sc, cycles, verify).await
            }
        };
        let mut row = tokio::time::timeout(Duration::from_secs(60), fut)
            .await
            .expect("sample timed out");
        let expected_evict =
            usize::from(sc == "cancel" || (label == "V2" && (sc == "trailing" || sc == "held_la")));
        assert_eq!(row["evictions"], expected_evict);
        assert_eq!(row["fetches"], expected_evict);
        assert_eq!(
            row["la_reexecutions"],
            if sc == "held_la" { expected_evict } else { 0 }
        );
        if sc == "held_la" {
            assert_eq!(row["la_executions"], 1 + expected_evict);
        }
        let expected_answer = match sc.as_str() {
            "normal" => "",
            "cancel" => "acked=false cancelled=true",
            "trailing" if label == "V2" => "acked=true cancelled=false",
            "held_la" if label == "V2" => "acked=true la_done=false",
            "held_la" => "acked=false la_done=false",
            _ => "acked=false cancelled=false",
        };
        assert_eq!(row["query_answer"], expected_answer);
        row["label"] = json!(label);
        row["scenario"] = json!(sc);
        row["cycles"] = json!(cycles);
        row["round"] = json!(round);
        row["sample"] = json!(i);
        row["warmup"] = json!(i < warmup);
        row["verify"] = json!(verify);
        writeln!(file, "{row}").unwrap();
        file.flush().unwrap();
    }
}
fn signal_external() -> workflow_command::Variant {
    SignalExternalWorkflowExecution {
        seq: 1,
        target: Some(
            signal_external_workflow_execution::Target::WorkflowExecution(
                NamespacedWorkflowExecution {
                    namespace: "default".to_string(),
                    workflow_id: "signal-target".to_string(),
                    run_id: "".to_string(),
                },
            ),
        ),
        signal_name: "wake".to_string(),
        ..Default::default()
    }
    .into()
}

fn memo() -> workflow_command::Variant {
    ModifyWorkflowProperties {
        upserted_memo: Some(Memo {
            fields: HashMap::from([("progress".to_string(), Payload::default())]),
        }),
    }
    .into()
}

/// Appends the events the server records for a workflow task completion, in command order.
fn commit(t: &mut TestHistoryBuilder, commands: &[Command], messages: &[ProtocolMessage]) {
    t.add_workflow_task_completed();
    let completed_id = t.current_event_id();
    let mut accepted_id = 0;
    for c in commands {
        match &c.attributes {
            Some(CmdAttrs::ModifyWorkflowPropertiesCommandAttributes(a)) => {
                t.add(WorkflowPropertiesModifiedEventAttributes {
                    workflow_task_completed_event_id: completed_id,
                    upserted_memo: a.upserted_memo.clone(),
                });
            }
            Some(CmdAttrs::StartTimerCommandAttributes(a)) => {
                t.add(TimerStartedEventAttributes {
                    timer_id: a.timer_id.clone(),
                    start_to_fire_timeout: a.start_to_fire_timeout,
                    workflow_task_completed_event_id: completed_id,
                });
            }
            Some(CmdAttrs::ProtocolMessageCommandAttributes(a)) => {
                let msg = messages
                    .iter()
                    .find(|m| m.id == a.message_id)
                    .expect("protocol message command must reference a sent message");
                if msg.id.ends_with("/accept") {
                    accepted_id = t.add_update_accepted(UPDATE_ID, "update_fn");
                } else {
                    t.add_update_completed(accepted_id);
                }
            }
            other => panic!("unexpected command {other:?}"),
        }
    }
}

/// Cold replay on a fresh replay worker. Returns ("ok" | "nde" | "error:...", activations).
async fn cold_replay(
    history: TestHistoryBuilder,
    mut lang: impl FnMut(&WorkflowActivation) -> Vec<workflow_command::Variant>,
) -> (String, usize) {
    let worker = init_replay_worker(ReplayWorkerInput::new(
        test_worker_cfg().build().unwrap(),
        stream::iter([HistoryForReplay::from(TestHistoryBuilder::from_history(
            history_of(&history).events,
        ))]),
    ))
    .unwrap();
    let mut verdict = "ok".to_string();
    let mut acts = 0;
    loop {
        let act = match worker.poll_workflow_activation().await {
            Ok(act) => act,
            Err(PollError::ShutDown) => break,
            Err(e) => {
                verdict = format!("error:{e:?}");
                break;
            }
        };
        acts += 1;
        let cmds = if let Some(reason) = act.eviction_reason() {
            if reason == EvictionReason::Nondeterminism {
                verdict = "nde".to_string();
            }
            vec![]
        } else {
            lang(&act)
        };
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(act.run_id, cmds))
            .await
            .unwrap();
    }
    (verdict, acts)
}

fn cmd_types(cmds: &[Command]) -> String {
    cmds.iter()
        .map(|c| {
            c.command_type()
                .as_str_name()
                .trim_start_matches("COMMAND_TYPE_")
        })
        .collect::<Vec<_>>()
        .join("+")
}

fn query_ids(activation: &WorkflowActivation) -> Vec<String> {
    activation
        .jobs
        .iter()
        .filter_map(|j| match &j.variant {
            Some(Job::QueryWorkflow(q)) => Some(q.query_id.clone()),
            _ => None,
        })
        .collect()
}

fn has_cmd(c: &WorkflowTaskCompletion, ty: CommandType) -> bool {
    c.commands.iter().any(|c| c.command_type() == ty)
}

fn record_completion(t: &mut TestHistoryBuilder, completion: &WorkflowTaskCompletion) {
    t.add_workflow_task_completed();
    if let Some(history_event::Attributes::WorkflowTaskCompletedEventAttributes(attrs)) =
        t.last_event().and_then(|e| e.attributes.as_mut())
    {
        attrs.sdk_metadata = Some(completion.sdk_metadata.clone());
    }
    let wft_completed_id = t.current_event_id();
    for command in &completion.commands {
        match command.attributes.clone().unwrap() {
            CmdAttrs::StartTimerCommandAttributes(timer) => {
                t.add_timer_started(timer.timer_id);
            }
            CmdAttrs::RecordMarkerCommandAttributes(marker) => {
                t.add(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes {
                        marker_name: marker.marker_name,
                        details: marker.details,
                        header: marker.header,
                        failure: marker.failure,
                        workflow_task_completed_event_id: wft_completed_id,
                    },
                ));
            }
            CmdAttrs::CompleteWorkflowExecutionCommandAttributes(_) => {
                t.add_workflow_execution_completed();
            }
            CmdAttrs::SignalExternalWorkflowExecutionCommandAttributes(signal) => {
                t.add_signal_wf(
                    signal.signal_name,
                    signal.execution.map(|e| e.workflow_id).unwrap_or_default(),
                    "",
                );
            }
            CmdAttrs::ModifyWorkflowPropertiesCommandAttributes(props) => {
                t.add(WorkflowPropertiesModifiedEventAttributes {
                    workflow_task_completed_event_id: wft_completed_id,
                    upserted_memo: props.upserted_memo,
                });
            }
            unexpected => panic!("Unexpected command: {unexpected:?}"),
        }
    }
}
