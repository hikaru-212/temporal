package tests

// Functional regression tests for temporalio/temporal#12880: a child completion addressed to a parent
// run that was reset and then deleted by retention.
//
//   - L1: R2 is reset from R1 after R1 started child C1, so R2 inherits C1's initiation and started
//     event by fork. After R1 is deleted, R2 must record C1's completion exactly once and no longer have
//     the child pending.
//   - X1: after R1 is deleted and R2 has closed, R3 reuses the parent workflow ID and initiates a child
//     with C1's workflow ID at the same initiated event ID and version; its start-child task is held so
//     the initiation stays initiated-only. R3 must never accept C1's completion.
//
// Retention is simulated with the admin force-delete, which only touches persistence, followed by
// CloseShard: retention also clears the cached workflow context, and the workflow cache key includes
// the shard owner, so the reloaded shard cannot serve a stale R1. CloseShard needs a dedicated cluster.
// The retention timer path itself is not exercised. Child-completion recovery is disabled so a
// re-dispatched START_CHILD_EXECUTION task cannot resend C1's completion.
//
// Assertions run only after C1's completion has settled: every RecordChildExecutionCompleted attempt is
// observed on the history server (request snapshotted before the handler rewrites
// ParentExecution.RunId, paired with its own response), and C1's exact close transfer task
// (CloseTransferTaskId) is acknowledged in its shard's persisted queue state (queues.IsTaskAcked), so
// no retry is outstanding. A server result alone is not enough, because the client may still see a
// transport error and retry. The child is placed on a different shard than the parent because a task
// counts as acknowledged only when no lower task of its shard is pending, and X1 keeps one pending.

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc/grpcfaults"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	rccWaitTimeout = 30 * time.Second
	rccWaitTick    = 100 * time.Millisecond
)

type ResetChildCompletionSuite struct {
	parallelsuite.Suite[*ResetChildCompletionSuite]
}

func TestResetChildCompletionSuite(t *testing.T) {
	parallelsuite.Run(t, &ResetChildCompletionSuite{})
}

// TestChildCompletionAfterReset_L1_DeletedBaseRun_ResetSuccessorLiveness covers the legitimate case:
// the pending child in R2 was inherited from R1 by fork, so C1's completion belongs to R2.
func (s *ResetChildCompletionSuite) TestChildCompletionAfterReset_L1_DeletedBaseRun_ResetSuccessorLiveness() {
	env := newRCCEnv(s.T())
	ctx := s.Context()
	t := s.T()
	observer := newRCCTransferTaskObserver(t, env)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	parentTV := testvars.New(s.T())
	childTV := rccChildTestVars(env, parentTV)

	// PARENT_CLOSE_POLICY_TERMINATE is the default policy. C1 must survive R1's reset-termination
	// because the parent close policy is skipped for a reset parent (AllowResetWithPendingChildren).
	fx := setupRCCResetWithStartedChild(ctx, t, env, observer, poller, parentTV, childTV, enumspb.PARENT_CLOSE_POLICY_TERMINATE)

	// R1 disappears as it would after retention; R2 is still running with C1 pending.
	deleteRCCBaseRun(ctx, t, env, fx)

	// C1 completes; its CLOSE_EXECUTION task sends RecordChildExecutionCompleted addressed to R1. This
	// returns only after that exact close task is acknowledged and every attempt has been answered.
	outcome := completeRCCChild(ctx, t, env, observer, poller, fx)
	// The acknowledgment must come from normal delivery, not from dropping or dead-lettering the task.
	assertRCCDeliveredOutcome(env, fx, outcome)

	// Contract: the legitimate reset successor R2 receives exactly one completion for the inherited
	// initiation, it references C1, and R2 no longer has that child pending.
	r2Events := rccHistory(ctx, t, env, fx.parentWorkflowID, fx.r2)
	completed := rccChildCompletedEvents(r2Events, fx.initiatedEventID)
	env.Len(completed, 1,
		"R2 must record exactly one ChildWorkflowExecutionCompleted for initiation %d (%s)", fx.initiatedEventID, outcome)
	attrs := completed[0].GetChildWorkflowExecutionCompletedEventAttributes()
	env.Equal(fx.initiatedEventID, attrs.GetInitiatedEventId())
	env.Equal(fx.childWorkflowID, attrs.GetWorkflowExecution().GetWorkflowId())
	env.Equal(fx.c1, attrs.GetWorkflowExecution().GetRunId())
	_, stillPending := rccDescribe(ctx, t, env, fx.parentWorkflowID, fx.r2).GetChildExecutionInfos()[fx.initiatedEventID]
	env.False(stillPending, "R2 must no longer have the child pending after its completion (%s)", outcome)
}

// TestChildCompletionAfterReset_X1_DeletedBaseRun_UnrelatedCurrentRunSafety covers the unsafe case:
// the current run of the parent workflow ID is an unrelated execution created by workflow ID reuse.
func (s *ResetChildCompletionSuite) TestChildCompletionAfterReset_X1_DeletedBaseRun_UnrelatedCurrentRunSafety() {
	env := newRCCEnv(s.T(),
		// R3 reuses the parent workflow ID right after R2 closes; remove the reuse rate limit.
		testcore.WithDynamicConfig(dynamicconfig.WorkflowIdReuseMinimalInterval, time.Duration(0)),
	)
	ctx := s.Context()
	t := s.T()
	observer := newRCCTransferTaskObserver(t, env)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
	parentTV := testvars.New(s.T())
	childTV := rccChildTestVars(env, parentTV)

	// ABANDON keeps C1 running after R2 closes.
	fx := setupRCCResetWithStartedChild(ctx, t, env, observer, poller, parentTV, childTV, enumspb.PARENT_CLOSE_POLICY_ABANDON)

	// R2 finishes. Its inherited child is abandoned, so C1 keeps running.
	pollRCCWorkflowTask(t, env, poller, parentTV, fx.r2, rccCompleteWorkflowCommand())
	observer.waitExecuted(ctx, env, rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, fx.parentWorkflowID, fx.r2}, "R2 CLOSE_EXECUTION")
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rccDescribeExecution(ctx, t, env, fx.parentWorkflowID, fx.r2).GetWorkflowExecutionInfo().GetStatus())
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rccDescribeExecution(ctx, t, env, fx.childWorkflowID, fx.c1).GetWorkflowExecutionInfo().GetStatus(),
		"C1 must keep running after R2 closes (ABANDON)")

	// R1 disappears as it would after retention. This happens before R3 starts so that the shard
	// close inside deleteRCCBaseRun never runs while a transfer worker is parked in the hold hook.
	deleteRCCBaseRun(ctx, t, env, fx)

	// R3: an unrelated execution reusing the parent workflow ID with an explicit reuse policy.
	r3 := startRCCParent(ctx, t, env, parentTV)
	env.NotEqual(fx.r1, r3)
	env.NotEqual(fx.r2, r3)
	r3StartChildKey := rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, fx.parentWorkflowID, r3}
	// Install the hold before R3's workflow task completes, i.e. before the task can exist.
	observer.holdTask(r3StartChildKey)
	pollRCCWorkflowTask(t, env, poller, parentTV, r3, rccStartChildCommand(childTV, enumspb.PARENT_CLOSE_POLICY_ABANDON))
	observer.waitHeld(env)

	// R3's pending initiation matches every identifier that C1's completion request carries
	// (parent workflow ID, ParentInitiatedId, ParentInitiatedVersion, child workflow ID).
	r3Events := rccHistory(ctx, t, env, fx.parentWorkflowID, r3)
	r3Initiated := rccEventByID(r3Events, fx.initiatedEventID)
	env.NotNil(r3Initiated, "R3 has no event at C1's ParentInitiatedId")
	env.Equal(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, r3Initiated.GetEventType(),
		"R3's event at C1's ParentInitiatedId must be a child initiation for this test to be meaningful")
	env.Equal(fx.childWorkflowID, r3Initiated.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId())
	env.Equal(fx.initiatedVersion, r3Initiated.GetVersion())
	assertRCCInitiatedOnly(ctx, t, env, fx, r3)

	// C1 completes; its CLOSE_EXECUTION task sends RecordChildExecutionCompleted addressed to R1. This
	// returns only after that exact close task is acknowledged and every attempt has been answered, so
	// an outstanding retry cannot make the safety assertions below pass early.
	outcome := completeRCCChild(ctx, t, env, observer, poller, fx)
	// The acknowledgment must come from an expected rejection, not from dropping or dead-lettering the
	// task after an abnormal failure.
	assertRCCRejectedOutcome(t, env, fx, outcome)

	// Safety: R3 never adopts C1.
	r3Events = rccHistory(ctx, t, env, fx.parentWorkflowID, r3)
	for _, event := range r3Events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
			enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
			env.Failf("unrelated run R3 recorded a child event",
				"event %d of type %v (%s)", event.GetEventId(), event.GetEventType(), outcome)
		default:
		}
	}
	assertRCCInitiatedOnly(ctx, t, env, fx, r3)
	env.Zero(observer.executedCount(r3StartChildKey), "R3's START_CHILD_EXECUTION task must still be held")
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		rccDescribeExecution(ctx, t, env, fx.parentWorkflowID, r3).GetWorkflowExecutionInfo().GetStatus())
}

// rccResetFixture describes R1 → R2 after R1 started child C1.
type rccResetFixture struct {
	childTV          *testvars.TestVars
	parentWorkflowID string
	childWorkflowID  string
	r1               string
	r2               string
	c1               string
	initiatedEventID int64
	initiatedVersion int64
	parentShardID    int32
	childShardID     int32
}

func newRCCEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	opts = append([]testcore.TestOption{
		// CloseShard (used to evict the deleted base run from the workflow cache) and the global
		// checkpoint settings below need a dedicated cluster.
		testcore.WithDedicatedCluster(),
		// The close-task barrier reads the persisted transfer queue state. By default it is checkpointed
		// every 30s and persisted at most every 5m; checkpoint every second and persist every checkpoint.
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, time.Second),
		testcore.WithDynamicConfig(dynamicconfig.ShardUpdateMinInterval, time.Duration(0)),
		// Pin the production default: the reset parent's close policy is skipped so C1 stays reconnectable.
		testcore.WithDynamicConfig(dynamicconfig.AllowResetWithPendingChildren, true),
		// R2's regenerated START_CHILD_EXECUTION task may run (or re-run after the shard reload) after C1
		// has completed. With recovery enabled it would refresh C1's tasks and send a second completion,
		// making the number of completion attempts nondeterministic.
		testcore.WithDynamicConfig(dynamicconfig.EnableChildWorkflowCompletionRecovery, false),
	}, opts...)
	return testcore.NewEnv(t, opts...)
}

// rccChildTestVars picks a child workflow ID whose history shard differs from the parent's. C1's
// close task is judged acknowledged from its shard's persisted transfer queue state, which never
// reports a task as acknowledged while a lower task key of the same shard is still pending; X1 keeps
// R3's START_CHILD_EXECUTION task (on the parent's shard) pending on purpose.
func rccChildTestVars(env *testcore.TestEnv, parentTV *testvars.TestVars) *testvars.TestVars {
	numShards := env.GetTestClusterConfig().HistoryConfig.NumHistoryShards
	env.Greater(numShards, int32(1), "the fixture needs at least two history shards")
	namespaceID := env.NamespaceID().String()
	parentShardID := common.WorkflowIDToHistoryShard(namespaceID, parentTV.WorkflowID(), numShards)
	for i := 0; i < 1000; i++ {
		childWorkflowID := fmt.Sprintf("%s-child-%d", parentTV.WorkflowID(), i)
		if common.WorkflowIDToHistoryShard(namespaceID, childWorkflowID, numShards) != parentShardID {
			return parentTV.
				WithWorkflowID(childWorkflowID).
				WithTaskQueue(parentTV.TaskQueue().GetName() + "-child")
		}
	}
	env.FailNow("no child workflow ID on a different shard than the parent was found")
	return nil
}

// setupRCCResetWithStartedChild runs:
//
//	R1: WorkflowExecutionStarted, WFT → [StartChild(K)] → StartChildWorkflowExecutionInitiated(N)
//	    START_CHILD_EXECUTION → ChildWorkflowExecutionStarted(C1), WFT → [] → WorkflowTaskCompleted(F)
//	Reset R1 at F → R2 (LCA = F-1 ≥ N), so R2 inherits initiation N and C1's started event by fork.
//
// It waits for R1's START_CHILD_EXECUTION, R1's CLOSE_EXECUTION (reset termination) and R2's
// regenerated START_CHILD_EXECUTION tasks, and checks the lineage and child identity of every run.
func setupRCCResetWithStartedChild(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	observer *rccTransferTaskObserver,
	poller *taskpoller.TaskPoller,
	parentTV *testvars.TestVars,
	childTV *testvars.TestVars,
	parentClosePolicy enumspb.ParentClosePolicy,
) rccResetFixture {
	numShards := env.GetTestClusterConfig().HistoryConfig.NumHistoryShards
	fx := rccResetFixture{
		childTV:          childTV,
		parentWorkflowID: parentTV.WorkflowID(),
		childWorkflowID:  childTV.WorkflowID(),
		parentShardID:    common.WorkflowIDToHistoryShard(env.NamespaceID().String(), parentTV.WorkflowID(), numShards),
		childShardID:     common.WorkflowIDToHistoryShard(env.NamespaceID().String(), childTV.WorkflowID(), numShards),
	}
	env.NotEqual(fx.parentShardID, fx.childShardID, "the child must live on a different shard than the parent")

	// R1 initiates C1.
	fx.r1 = startRCCParent(ctx, t, env, parentTV)
	pollRCCWorkflowTask(t, env, poller, parentTV, fx.r1, rccStartChildCommand(childTV, parentClosePolicy))

	r1Events := rccHistory(ctx, t, env, fx.parentWorkflowID, fx.r1)
	var initiated *historypb.HistoryEvent
	for _, event := range r1Events {
		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			env.Nil(initiated, "R1 must initiate exactly one child")
			initiated = event
		}
	}
	env.NotNil(initiated, "R1 did not initiate the child")
	env.Equal(fx.childWorkflowID, initiated.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId())
	fx.initiatedEventID = initiated.GetEventId()
	fx.initiatedVersion = initiated.GetVersion()

	// Wait until R1's START_CHILD_EXECUTION task has run (child start, ChildWorkflowExecutionStarted,
	// and the child's first workflow task) and the started event is durable in R1's history.
	r1StartChildKey := rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, fx.parentWorkflowID, fx.r1}
	var childStarted *historypb.HistoryEvent
	rccPoll(ctx, env, "R1 records ChildWorkflowExecutionStarted", func(ctx context.Context) (bool, string, error) {
		executed := observer.executedCount(r1StartChildKey)
		if executed == 0 {
			return false, "R1's START_CHILD_EXECUTION task has not returned yet", nil
		}
		events, err := tryRCCHistory(ctx, env, fx.parentWorkflowID, fx.r1)
		if err != nil {
			if common.IsNotFoundError(err) {
				// R1 exists at this point; NotFound is not a convergence state.
				return false, "", rccUnexpected(err)
			}
			return false, "reading R1 history", err
		}
		childStarted = rccChildStartedEvent(events, fx.initiatedEventID)
		return childStarted != nil, fmt.Sprintf("START_CHILD_EXECUTION returned %d time(s); R1 has %d events and no ChildWorkflowExecutionStarted", executed, len(events)), nil
	})
	fx.c1 = childStarted.GetChildWorkflowExecutionStartedEventAttributes().GetWorkflowExecution().GetRunId()
	env.NotEmpty(fx.c1)

	// ChildWorkflowExecutionStarted scheduled a new workflow task for R1; complete it without commands
	// to obtain a reset point after the child initiation.
	pollRCCWorkflowTask(t, env, poller, parentTV, fx.r1)
	r1Events = rccHistory(ctx, t, env, fx.parentWorkflowID, fx.r1)
	var resetFinishEventID int64
	for _, event := range r1Events {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			resetFinishEventID = event.GetEventId()
		}
	}
	env.Greater(resetFinishEventID, childStarted.GetEventId(), "no workflow task completed after the child started")
	lcaEvent := rccEventByID(r1Events, resetFinishEventID-1)
	env.NotNil(lcaEvent)
	env.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, lcaEvent.GetEventType(), "reset point must follow a started workflow task")
	env.GreaterOrEqual(resetFinishEventID-1, fx.initiatedEventID, "the reset point must keep the child initiation")

	// C1 records exactly the identifiers its CLOSE_EXECUTION task will send to the parent.
	c1State := rccDescribe(ctx, t, env, fx.childWorkflowID, fx.c1)
	env.Equal(fx.parentWorkflowID, c1State.GetExecutionInfo().GetParentWorkflowId())
	env.Equal(fx.r1, c1State.GetExecutionInfo().GetParentRunId())
	env.Equal(fx.initiatedEventID, c1State.GetExecutionInfo().GetParentInitiatedId())
	env.Equal(fx.initiatedVersion, c1State.GetExecutionInfo().GetParentInitiatedVersion())
	env.Equal(fx.c1, c1State.GetExecutionState().GetFirstExecutionRunId())

	// Reset R1 → R2 after the child initiation.
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: fx.parentWorkflowID, RunId: fx.r1},
		Reason:                    "reset after child initiation",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: resetFinishEventID,
	})
	require.NoError(t, err)
	fx.r2 = resetResp.GetRunId()
	env.NotEmpty(fx.r2)
	env.NotEqual(fx.r1, fx.r2)

	// R1's reset-termination CLOSE_EXECUTION task must have run (and skipped the parent close policy)
	// before R1 is deleted. R2's rebuild runs a full task refresh with no previous pending children, so a
	// START_CHILD_EXECUTION task is regenerated for the inherited, already-started child. That task only
	// schedules C1's first workflow task, which is a no-op because C1 already has one; wait for it so it
	// cannot interleave with C1's completion.
	observer.waitExecuted(ctx, env, rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, fx.parentWorkflowID, fx.r1}, "R1 CLOSE_EXECUTION")
	observer.waitExecuted(ctx, env, rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION, fx.parentWorkflowID, fx.r2}, "R2 START_CHILD_EXECUTION")

	// Lineage: R2 is the reset successor of R1.
	r1State := rccDescribe(ctx, t, env, fx.parentWorkflowID, fx.r1)
	env.Equal(fx.r2, r1State.GetExecutionInfo().GetResetRunId())
	r2State := rccDescribe(ctx, t, env, fx.parentWorkflowID, fx.r2)
	env.Equal(fx.r1, r2State.GetExecutionInfo().GetBaseExecutionInfo().GetRunId())
	env.Equal(resetFinishEventID-1, r2State.GetExecutionInfo().GetBaseExecutionInfo().GetLowestCommonAncestorEventId())
	env.Equal(fx.r1, r2State.GetExecutionInfo().GetOriginalExecutionRunId())
	r2Events := rccHistory(ctx, t, env, fx.parentWorkflowID, fx.r2)
	env.NotNil(rccResetMarker(r2Events, fx.r1, fx.r2), "R2 has no reset marker for R1 → R2")

	// Child identity: R2 inherited initiation N and C1's started identity.
	r2Initiated := rccEventByID(r2Events, fx.initiatedEventID)
	env.NotNil(r2Initiated)
	env.Equal(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, r2Initiated.GetEventType())
	env.Equal(fx.childWorkflowID, r2Initiated.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetWorkflowId())
	r2Child, ok := r2State.GetChildExecutionInfos()[fx.initiatedEventID]
	env.True(ok, "R2 must inherit the pending child")
	env.NotEqual(common.EmptyEventID, r2Child.GetStartedEventId(), "R2's inherited child must be started")
	env.Equal(fx.childWorkflowID, r2Child.GetStartedWorkflowId())
	env.Equal(fx.c1, r2Child.GetStartedRunId())

	// The regenerated START_CHILD_EXECUTION task did not create a second child execution.
	c1Desc := rccDescribeExecution(ctx, t, env, fx.childWorkflowID, "")
	env.Equal(fx.c1, c1Desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(), "C1 must still be the child's current run")
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, c1Desc.GetWorkflowExecutionInfo().GetStatus(), "C1 must keep running after the reset")

	return fx
}

// deleteRCCBaseRun deletes R1 the way retention would and proves it is absent before C1 completes.
func deleteRCCBaseRun(ctx context.Context, t *testing.T, env *testcore.TestEnv, fx rccResetFixture) {
	_, err := env.AdminClient().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: fx.parentWorkflowID, RunId: fx.r1},
		Archetype: chasm.WorkflowArchetype,
	})
	require.NoError(t, err)

	// R1's mutable state row is gone from persistence. NotFound is the expected end state; anything
	// else is recorded and reported if the wait times out.
	execMgr := env.GetTestCluster().TestBase().ExecutionManager
	rccPoll(ctx, env, "R1 is removed from persistence", func(ctx context.Context) (bool, string, error) {
		_, err := execMgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     fx.parentShardID,
			NamespaceID: env.NamespaceID().String(),
			WorkflowID:  fx.parentWorkflowID,
			RunID:       fx.r1,
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		if common.IsNotFoundError(err) {
			return true, "", nil
		}
		return false, "R1's mutable state is still readable from persistence", err
	})

	// The admin force-delete does not touch the workflow cache, while retention deletion clears the
	// cached context. Reloading the shard gives it a new owner, which the workflow cache key includes,
	// so a stale cached R1 can no longer be served.
	env.CloseShard(env.NamespaceID().String(), fx.parentWorkflowID)

	// R1 is absent through the history service as well. Shard-reload errors (for example
	// ShardOwnershipLost or Unavailable) are transient here and only recorded.
	rccPoll(ctx, env, "R1 is absent through the history service", func(ctx context.Context) (bool, string, error) {
		_, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: fx.parentWorkflowID, RunId: fx.r1},
			Archetype: chasm.WorkflowArchetype,
		})
		if common.IsNotFoundError(err) {
			return true, "", nil
		}
		if err == nil {
			return false, "the history service still returns R1 after the shard close", nil
		}
		return false, "describing R1", err
	})
	// The reloaded shard serves R2. NotFound would mean the fixture lost R2 and is not transient.
	rccPoll(ctx, env, "the parent's shard reloads and serves R2", func(ctx context.Context) (bool, string, error) {
		_, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: fx.parentWorkflowID, RunId: fx.r2},
			Archetype: chasm.WorkflowArchetype,
		})
		if err == nil {
			return true, "", nil
		}
		if common.IsNotFoundError(err) {
			return false, "", rccUnexpected(err)
		}
		return false, "describing R2 after the shard close", err
	})
}

// completeRCCChild completes C1 and returns only when its completion delivery has settled:
//
//  1. C1's exact close transfer task (its persisted CloseTransferTaskId) has run through the real
//     executor,
//  2. that task is acknowledged in the child shard's persisted transfer queue state, so it is no
//     longer pending: no retry is outstanding and it will never be re-dispatched, and
//  3. every RecordChildExecutionCompleted attempt observed on the history server has a response, so
//     no handler can still commit a completion after the barrier.
//
// It then verifies that every observed attempt carried C1's exact completion request identity.
func completeRCCChild(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	observer *rccTransferTaskObserver,
	poller *taskpoller.TaskPoller,
	fx rccResetFixture,
) rccCompletionEvidence {
	pollRCCWorkflowTask(t, env, poller, fx.childTV, fx.c1, rccCompleteWorkflowCommand())
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		rccDescribeExecution(ctx, t, env, fx.childWorkflowID, fx.c1).GetWorkflowExecutionInfo().GetStatus())

	closeTaskID := rccDescribe(ctx, t, env, fx.childWorkflowID, fx.c1).GetExecutionInfo().GetCloseTransferTaskId()
	env.NotZero(closeTaskID, "C1 has no persisted CloseTransferTaskId")
	closeTaskKey := rccTaskKey{enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION, fx.childWorkflowID, fx.c1}
	closeTask := &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(env.NamespaceID().String(), fx.childWorkflowID, fx.c1),
		TaskID:      closeTaskID,
	}
	transferCategoryID := int32(tasks.CategoryTransfer.ID())

	rccPoll(ctx, env, "C1's close transfer task is acknowledged", func(ctx context.Context) (bool, string, error) {
		executedIDs := observer.executedTaskIDs(closeTaskKey)
		if !slices.Contains(executedIDs, closeTaskID) {
			return false, fmt.Sprintf("close task %d has not returned from the executor (returned task IDs: %v; %s)",
				closeTaskID, executedIDs, observer.completionEvidence()), nil
		}
		resp, err := env.AdminClient().GetShard(ctx, &adminservice.GetShardRequest{ShardId: fx.childShardID})
		if err != nil {
			return false, fmt.Sprintf("reading persisted shard %d", fx.childShardID), err
		}
		queueState, ok := resp.GetShardInfo().GetQueueStates()[transferCategoryID]
		if !ok {
			return false, fmt.Sprintf("shard %d has no persisted transfer queue state yet", fx.childShardID), nil
		}
		if !queues.IsTaskAcked(closeTask, queueState) {
			return false, fmt.Sprintf("close task %d is still pending in shard %d's persisted transfer queue state %v (%s)",
				closeTaskID, fx.childShardID, queueState, observer.completionEvidence()), nil
		}
		evidence := observer.completionEvidence()
		if unanswered := evidence.unanswered(); unanswered > 0 {
			return false, fmt.Sprintf("close task %d is acknowledged but %d completion attempt(s) have no response yet (%s)",
				closeTaskID, unanswered, evidence), nil
		}
		return true, "", nil
	})

	evidence := observer.completionEvidence()
	assertRCCCompletionRequests(env, fx, evidence)
	return evidence
}

// assertRCCCompletionRequests fails the fixture unless C1's completion was attempted at least once and
// every observed attempt carried C1's original request identity and received a response.
func assertRCCCompletionRequests(env *testcore.TestEnv, fx rccResetFixture, evidence rccCompletionEvidence) {
	env.Empty(evidence.unpairedResponses, "fixture: completion responses without an observed request (%s)", evidence)
	env.NotEmpty(evidence.attempts, "fixture: no RecordChildExecutionCompleted request was observed for C1")
	expected := rccCompletionRequest{
		method:                 historyservice.HistoryService_RecordChildExecutionCompleted_FullMethodName,
		namespaceID:            env.NamespaceID().String(),
		parentWorkflowID:       fx.parentWorkflowID,
		parentRunID:            fx.r1,
		parentInitiatedID:      fx.initiatedEventID,
		parentInitiatedVersion: fx.initiatedVersion,
		childWorkflowID:        fx.childWorkflowID,
		childRunID:             fx.c1,
		childFirstRunID:        fx.c1,
		completionEventType:    enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	}
	for i, attempt := range evidence.attempts {
		env.Equal(expected, attempt.request, "fixture: completion attempt %d does not carry C1's request identity", i)
		env.True(attempt.responded, "fixture: completion attempt %d has no response", i)
	}
}

// Outcome validation.
//
// The barrier proves that C1's close task is acknowledged, but the task framework also acknowledges a
// task it drops or dead-letters after an abnormal failure (executableImpl.HandleErr). The completion
// RPC is the close task's only remote step, and C1 has no children whose close policy could fail
// afterwards, so a normal acknowledgment follows only an attempt whose result processCloseExecution
// accepts: nil, or NotFound. The validators therefore require that:
//
//   - no attempt failed abnormally: DataLoss, Internal, or a terminal task error. These are the errors
//     the framework drops or dead-letters instead of retrying, and no expected completion path returns
//     them;
//   - the final attempt, which is the last one the close task issued before it was acknowledged, has a
//     result that the respective contract explains. An earlier acceptable result never masks a later
//     abnormal one.
//
// Earlier attempts with retryable errors are kept only as diagnostics.

// rccIsAbnormalCompletionError reports errors that make the task framework drop or dead-letter the
// close task instead of retrying it (see executableImpl.isUnexpectedNonRetryableError).
func rccIsAbnormalCompletionError(err error) bool {
	if err == nil {
		return false
	}
	var terminal queues.MaybeTerminalTaskError
	return errors.As(err, new(*serviceerror.DataLoss)) ||
		errors.As(err, new(*serviceerror.Internal)) ||
		(errors.As(err, &terminal) && terminal.IsTerminalTaskError())
}

// rccIsNotFoundWithMessage reports whether err is a NotFound with exactly the given message, so that
// a NotFound from an unrelated source is not mistaken for an expected rejection.
func rccIsNotFoundWithMessage(err error, message string) bool {
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound) && notFound.Error() == message
}

// assertRCCDeliveredOutcome (L1) requires that exactly one attempt recorded the completion (nil) and
// that the final attempt is either that success or, after a retry whose earlier success the client
// did not see, a duplicate rejection explained by the child no longer being pending: the parent's
// child-not-found rejection, or the original missing-R1 NotFound that the reset-successor fallback
// returns once R2 no longer has the started child.
func assertRCCDeliveredOutcome(env *testcore.TestEnv, fx rccResetFixture, evidence rccCompletionEvidence) {
	env.NotEmpty(evidence.attempts, "no completion attempt was observed (%s)", evidence)
	successIndex := -1
	for i, attempt := range evidence.attempts {
		env.False(rccIsAbnormalCompletionError(attempt.err),
			"completion attempt %d failed abnormally; the close task may have been dropped or dead-lettered (%s)", i, evidence)
		if attempt.err == nil {
			env.Equal(-1, successIndex, "more than one completion attempt succeeded (%s)", evidence)
			successIndex = i
		}
	}
	env.NotEqual(-1, successIndex, "no completion attempt succeeded (%s)", evidence)
	final := len(evidence.attempts) - 1
	if final != successIndex {
		finalErr := evidence.attempts[final].err
		missingBaseRun := fmt.Sprintf("workflow execution not found for workflow ID %q and run ID %q", fx.parentWorkflowID, fx.r1)
		env.True(
			rccIsNotFoundWithMessage(finalErr, consts.ErrChildExecutionNotFound.Error()) ||
				rccIsNotFoundWithMessage(finalErr, missingBaseRun),
			"the final completion attempt after the successful attempt %d is not an expected duplicate rejection: %v (%s)",
			successIndex, finalErr, evidence)
	}
}

// assertRCCRejectedOutcome (X1) requires that the completion was handled and rejected the way the
// existing contract allows when no legitimate recipient exists:
//
//   - the deleted parent run R1 is reported missing (the persistence layer's normalized NotFound for
//     exactly R1),
//   - the parent run that handled the request is closed (ErrWorkflowCompleted), or
//   - the handling parent has no matching pending child (ErrChildExecutionNotFound).
//
// A nil result is never accepted: under the existing contract it means the completion was recorded,
// and in X1 no run may record it (R1 is deleted, R2 is closed, R3 is unrelated).
func assertRCCRejectedOutcome(t *testing.T, env *testcore.TestEnv, fx rccResetFixture, evidence rccCompletionEvidence) {
	env.NotEmpty(evidence.attempts, "no completion attempt was observed (%s)", evidence)
	for i, attempt := range evidence.attempts {
		env.False(rccIsAbnormalCompletionError(attempt.err),
			"completion attempt %d failed abnormally; the close task may have been dropped or dead-lettered (%s)", i, evidence)
		require.Error(t, attempt.err, "completion attempt %d recorded the completion although X1 has no legitimate recipient (%s)", i, evidence)
	}
	finalErr := evidence.attempts[len(evidence.attempts)-1].err
	missingBaseRun := fmt.Sprintf("workflow execution not found for workflow ID %q and run ID %q", fx.parentWorkflowID, fx.r1)
	env.True(
		rccIsNotFoundWithMessage(finalErr, missingBaseRun) ||
			rccIsNotFoundWithMessage(finalErr, consts.ErrWorkflowCompleted.Error()) ||
			rccIsNotFoundWithMessage(finalErr, consts.ErrChildExecutionNotFound.Error()),
		"the final completion attempt was not an expected rejection: %v (%s)", finalErr, evidence)
}

func assertRCCInitiatedOnly(ctx context.Context, t *testing.T, env *testcore.TestEnv, fx rccResetFixture, runID string) {
	state := rccDescribe(ctx, t, env, fx.parentWorkflowID, runID)
	ci, ok := state.GetChildExecutionInfos()[fx.initiatedEventID]
	env.True(ok, "run %s must have a pending child at event %d", runID, fx.initiatedEventID)
	env.Equal(fx.childWorkflowID, ci.GetStartedWorkflowId())
	env.Equal(common.EmptyEventID, ci.GetStartedEventId(), "run %s's child must be initiated-only", runID)
	env.Empty(ci.GetStartedRunId(), "run %s's child must not have a started run ID", runID)
}

func startRCCParent(ctx context.Context, t *testing.T, env *testcore.TestEnv, parentTV *testvars.TestVars) string {
	resp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:             uuid.NewString(),
		Namespace:             env.Namespace().String(),
		WorkflowId:            parentTV.WorkflowID(),
		WorkflowType:          parentTV.WorkflowType(),
		TaskQueue:             parentTV.TaskQueue(),
		WorkflowRunTimeout:    durationpb.New(10 * time.Minute),
		WorkflowTaskTimeout:   durationpb.New(time.Minute),
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
	})
	require.NoError(t, err)
	env.NotEmpty(resp.GetRunId())
	return resp.GetRunId()
}

// pollRCCWorkflowTask polls one workflow task from tv's task queue, verifies that it belongs to the
// expected run, and completes it with the given commands (none completes it without commands).
func pollRCCWorkflowTask(
	t *testing.T,
	env *testcore.TestEnv,
	poller *taskpoller.TaskPoller,
	tv *testvars.TestVars,
	expectedRunID string,
	commands ...*commandpb.Command,
) {
	_, err := poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			env.Equal(tv.WorkflowID(), task.GetWorkflowExecution().GetWorkflowId(), "polled a workflow task of an unexpected workflow")
			env.Equal(expectedRunID, task.GetWorkflowExecution().GetRunId(), "polled a workflow task of an unexpected run")
			return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: commands}, nil
		})
	require.NoError(t, err)
}

func rccStartChildCommand(
	childTV *testvars.TestVars,
	parentClosePolicy enumspb.ParentClosePolicy,
) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId:            childTV.WorkflowID(),
				WorkflowType:          &commonpb.WorkflowType{Name: childTV.WorkflowID() + "-type"},
				TaskQueue:             childTV.TaskQueue(),
				WorkflowRunTimeout:    durationpb.New(10 * time.Minute),
				WorkflowTaskTimeout:   durationpb.New(time.Minute),
				ParentClosePolicy:     parentClosePolicy,
				WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			},
		},
	}
}

func rccCompleteWorkflowCommand() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

func rccDescribe(ctx context.Context, t *testing.T, env *testcore.TestEnv, workflowID, runID string) *persistencespb.WorkflowMutableState {
	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		Archetype: chasm.WorkflowArchetype,
	})
	require.NoError(t, err)
	env.NotNil(resp.GetDatabaseMutableState())
	return resp.GetDatabaseMutableState()
}

func rccDescribeExecution(ctx context.Context, t *testing.T, env *testcore.TestEnv, workflowID, runID string) *workflowservice.DescribeWorkflowExecutionResponse {
	resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	require.NoError(t, err)
	return resp
}

func rccHistory(ctx context.Context, t *testing.T, env *testcore.TestEnv, workflowID, runID string) []*historypb.HistoryEvent {
	events, err := tryRCCHistory(ctx, env, workflowID, runID)
	require.NoError(t, err)
	return events
}

func tryRCCHistory(ctx context.Context, env *testcore.TestEnv, workflowID, runID string) ([]*historypb.HistoryEvent, error) {
	var events []*historypb.HistoryEvent
	var pageToken []byte
	for {
		resp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:       env.Namespace().String(),
			Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			NextPageToken:   pageToken,
			MaximumPageSize: 100,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.GetHistory().GetEvents()...)
		pageToken = resp.GetNextPageToken()
		if len(pageToken) == 0 {
			return events, nil
		}
	}
}

func rccEventByID(events []*historypb.HistoryEvent, eventID int64) *historypb.HistoryEvent {
	for _, event := range events {
		if event.GetEventId() == eventID {
			return event
		}
	}
	return nil
}

func rccChildStartedEvent(events []*historypb.HistoryEvent, initiatedEventID int64) *historypb.HistoryEvent {
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED &&
			event.GetChildWorkflowExecutionStartedEventAttributes().GetInitiatedEventId() == initiatedEventID {
			return event
		}
	}
	return nil
}

func rccChildCompletedEvents(events []*historypb.HistoryEvent, initiatedEventID int64) []*historypb.HistoryEvent {
	var completed []*historypb.HistoryEvent
	for _, event := range events {
		if event.GetEventType() == enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED &&
			event.GetChildWorkflowExecutionCompletedEventAttributes().GetInitiatedEventId() == initiatedEventID {
			completed = append(completed, event)
		}
	}
	return completed
}

func rccResetMarker(events []*historypb.HistoryEvent, baseRunID, newRunID string) *historypb.HistoryEvent {
	for _, event := range events {
		attrs := event.GetWorkflowTaskFailedEventAttributes()
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED &&
			attrs.GetCause() == enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW &&
			attrs.GetBaseRunId() == baseRunID &&
			attrs.GetNewRunId() == newRunID {
			return event
		}
	}
	return nil
}

// rccTaskKey identifies a transfer task by type, workflow ID and run ID. The hook itself is
// namespace-scoped by InjectHook, so the namespace is implied.
type rccTaskKey struct {
	taskType   enumsspb.TaskType
	workflowID string
	runID      string
}

// rccCompletionRequest is an immutable snapshot of a RecordChildExecutionCompleted request, taken
// before the handler runs.
type rccCompletionRequest struct {
	method                 string
	namespaceID            string
	parentWorkflowID       string
	parentRunID            string
	parentInitiatedID      int64
	parentInitiatedVersion int64
	childWorkflowID        string
	childRunID             string
	childFirstRunID        string
	completionEventType    enumspb.EventType
}

// rccCompletionAttempt pairs one observed request with its own response.
type rccCompletionAttempt struct {
	request   rccCompletionRequest
	responded bool
	err       error
}

// rccCompletionEvidence is a copy of every RecordChildExecutionCompleted attempt observed so far.
type rccCompletionEvidence struct {
	attempts          []rccCompletionAttempt
	unpairedResponses []string
}

func (e rccCompletionEvidence) unanswered() int {
	n := 0
	for _, attempt := range e.attempts {
		if !attempt.responded {
			n++
		}
	}
	return n
}

func (e rccCompletionEvidence) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d completion attempt(s)", len(e.attempts))
	for i, attempt := range e.attempts {
		outcome := "no response yet"
		if attempt.responded {
			outcome = fmt.Sprintf("handler error: %v", attempt.err)
		}
		fmt.Fprintf(&b, "; #%d parent %s/%s initiated %d@v%d child %s/%s first %s: %s",
			i, attempt.request.parentWorkflowID, attempt.request.parentRunID, attempt.request.parentInitiatedID,
			attempt.request.parentInitiatedVersion, attempt.request.childWorkflowID, attempt.request.childRunID,
			attempt.request.childFirstRunID, outcome)
	}
	if len(e.unpairedResponses) > 0 {
		fmt.Fprintf(&b, "; unpaired responses: %v", e.unpairedResponses)
	}
	return b.String()
}

// rccTransferTaskObserver observes two things:
//
//   - Transfer tasks, through HistoryTransferTaskInterceptor. It records which task IDs returned from
//     the real executor per task key (a return does not mean success: the executor's error is not
//     visible to the hook), and it can hold one task key, parking that task before it executes.
//   - RecordChildExecutionCompleted calls on the history server, through request and response
//     callbacks on the gRPC fault generator. The request callback snapshots the original request
//     before the handler runs; the response callback, invoked by the interceptor with the same
//     request object after the handler returns, records that attempt's handler error. Neither
//     callback ever injects or changes a fault.
type rccTransferTaskObserver struct {
	mu       sync.Mutex
	executed map[rccTaskKey][]int64
	holding  bool
	holdKey  rccTaskKey
	// attempts lists every observed completion attempt in request order.
	attempts []*rccCompletionAttempt
	// inflight maps a request object to its attempt until its response is observed. The server
	// unmarshals a new request object per call, so the pointer identifies one attempt.
	inflight          map[*historyservice.RecordChildExecutionCompletedRequest]*rccCompletionAttempt
	unpairedResponses []string

	held        chan struct{}
	heldOnce    sync.Once
	release     chan struct{}
	releaseOnce sync.Once
}

func newRCCTransferTaskObserver(t *testing.T, env *testcore.TestEnv) *rccTransferTaskObserver {
	o := &rccTransferTaskObserver{
		executed: make(map[rccTaskKey][]int64),
		inflight: make(map[*historyservice.RecordChildExecutionCompletedRequest]*rccCompletionAttempt),
		held:     make(chan struct{}),
		release:  make(chan struct{}),
	}
	env.InjectHook(testhooks.NewHook(testhooks.HistoryTransferTaskInterceptor, o.intercept))
	// Registered directly on the generator rather than through env.InjectRequestFault or
	// env.InjectResponseFault, which fail the test when their callback never injects a fault; these
	// callbacks only observe.
	generator := env.GetTestCluster().Host().GetGRPCFaultGenerator()
	scope := grpcfaults.Scope{NamespaceID: env.NamespaceID()}
	t.Cleanup(generator.RegisterRequestCallback(scope, o.observeRequest))
	t.Cleanup(generator.RegisterResponseCallback(scope, o.observeResponse))
	// t.Cleanup is LIFO, so this runs before the callbacks are unregistered and before InjectHook
	// removes the interceptor: a parked task is always released, including when the test fails early.
	t.Cleanup(o.releaseHeld)
	return o
}

func (o *rccTransferTaskObserver) intercept(task tasks.Task, execute func()) {
	key := rccTaskKey{taskType: task.GetType(), workflowID: task.GetWorkflowID(), runID: task.GetRunID()}
	if o.shouldHold(key) {
		o.heldOnce.Do(func() { close(o.held) })
		<-o.release
		// Released at test cleanup without executing: the observation window is over, and running the
		// task now would act on a cluster that is being torn down. Returning without calling execute
		// yields a zero ExecuteResponse (ExecutionErr == nil), so the task is acknowledged, not retried.
		return
	}
	execute()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.executed[key] = append(o.executed[key], task.GetTaskID())
}

func (o *rccTransferTaskObserver) observeRequest(_ context.Context, operation string, req any) *grpcfaults.Outcome {
	request, ok := req.(*historyservice.RecordChildExecutionCompletedRequest)
	if !ok {
		return nil
	}
	attempt := &rccCompletionAttempt{request: rccCompletionRequest{
		method:                 operation,
		namespaceID:            request.GetNamespaceId(),
		parentWorkflowID:       request.GetParentExecution().GetWorkflowId(),
		parentRunID:            request.GetParentExecution().GetRunId(),
		parentInitiatedID:      request.GetParentInitiatedId(),
		parentInitiatedVersion: request.GetParentInitiatedVersion(),
		childWorkflowID:        request.GetChildExecution().GetWorkflowId(),
		childRunID:             request.GetChildExecution().GetRunId(),
		childFirstRunID:        request.GetChildFirstExecutionRunId(),
		completionEventType:    request.GetCompletionEvent().GetEventType(),
	}}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts = append(o.attempts, attempt)
	o.inflight[request] = attempt
	return nil
}

func (o *rccTransferTaskObserver) observeResponse(_ context.Context, operation string, req, _ any, err error) *grpcfaults.Outcome {
	request, ok := req.(*historyservice.RecordChildExecutionCompletedRequest)
	if !ok {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	attempt, found := o.inflight[request]
	if !found {
		// Values read here may already be rewritten by the handler; they are diagnostics only.
		o.unpairedResponses = append(o.unpairedResponses, fmt.Sprintf("%s child %s/%s: %v",
			operation, request.GetChildExecution().GetWorkflowId(), request.GetChildExecution().GetRunId(), err))
		return nil
	}
	delete(o.inflight, request)
	attempt.responded = true
	attempt.err = err
	return nil
}

func (o *rccTransferTaskObserver) completionEvidence() rccCompletionEvidence {
	o.mu.Lock()
	defer o.mu.Unlock()
	evidence := rccCompletionEvidence{
		attempts:          make([]rccCompletionAttempt, 0, len(o.attempts)),
		unpairedResponses: slices.Clone(o.unpairedResponses),
	}
	for _, attempt := range o.attempts {
		evidence.attempts = append(evidence.attempts, *attempt)
	}
	return evidence
}

func (o *rccTransferTaskObserver) shouldHold(key rccTaskKey) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.holding && key == o.holdKey
}

func (o *rccTransferTaskObserver) holdTask(key rccTaskKey) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.holding = true
	o.holdKey = key
}

func (o *rccTransferTaskObserver) executedCount(key rccTaskKey) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.executed[key])
}

func (o *rccTransferTaskObserver) executedTaskIDs(key rccTaskKey) []int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return slices.Clone(o.executed[key])
}

// waitExecuted waits until at least one execution of the task has returned from the real executor.
// This is a sequencing aid for fixture setup, not evidence that the task succeeded.
func (o *rccTransferTaskObserver) waitExecuted(ctx context.Context, env *testcore.TestEnv, key rccTaskKey, what string) {
	rccPoll(ctx, env, what+" returns from the executor", func(context.Context) (bool, string, error) {
		return o.executedCount(key) > 0, what + " has not returned from the executor", nil
	})
}

// waitHeld waits until the held task key has been dispatched to the executor and parked.
func (o *rccTransferTaskObserver) waitHeld(env *testcore.TestEnv) {
	select {
	case <-o.held:
	case <-time.After(rccWaitTimeout):
		env.FailNow("the held transfer task was never dispatched")
	}
}

func (o *rccTransferTaskObserver) releaseHeld() {
	o.releaseOnce.Do(func() { close(o.release) })
}

// rccUnexpectedError marks a probe error that cannot be part of convergence; rccPoll fails on it at once.
type rccUnexpectedError struct {
	err error
}

func (e rccUnexpectedError) Error() string { return "unexpected: " + e.err.Error() }

func (e rccUnexpectedError) Unwrap() error { return e.err }

func rccUnexpected(err error) error {
	return rccUnexpectedError{err: err}
}

// rccPoll calls probe until it reports done. The whole wait, including every probe call, is bounded
// by rccWaitTimeout through the context passed to probe. probe returns whether the condition holds,
// a description of the state it observed, and the error of the call it made, if any. Errors are
// recorded and polling continues, except errors marked with rccUnexpected and errors that cannot be
// transient, which fail the fixture immediately. On timeout the fixture fails with the last state and
// the last error.
func rccPoll(ctx context.Context, env *testcore.TestEnv, what string, probe func(context.Context) (bool, string, error)) {
	pollCtx, cancel := context.WithTimeout(ctx, rccWaitTimeout)
	defer cancel()
	tick := time.NewTicker(rccWaitTick)
	defer tick.Stop()

	var lastState string
	var lastErr error
	probes := 0
	for {
		probes++
		done, state, err := probe(pollCtx)
		if done {
			return
		}
		if state != "" {
			lastState = state
		}
		if err != nil {
			lastErr = err
			if errors.As(err, new(rccUnexpectedError)) || rccIsNonTransient(err) {
				env.FailNow(fmt.Sprintf("fixture: %s: unexpected error after %d probe(s): %v (last state: %s)",
					what, probes, err, lastState))
			}
		}
		select {
		case <-tick.C:
		case <-pollCtx.Done():
			env.FailNow(fmt.Sprintf("fixture: %s: not reached within %v after %d probe(s); last state: %s; last error: %v",
				what, rccWaitTimeout, probes, lastState, lastErr))
		}
	}
}

func rccIsNonTransient(err error) bool {
	return errors.As(err, new(*serviceerror.InvalidArgument)) ||
		errors.As(err, new(*serviceerror.PermissionDenied)) ||
		errors.As(err, new(*serviceerror.Unimplemented))
}
