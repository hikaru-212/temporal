package recordchildworkflowcompleted

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// maxResetRedirectCount is the number of times we follow the reset run ID to forward the request to the new parent.
// This redirection happens only when a workflow is reset and in most cases it's 1 or 2 hops.
// maxResetRedirectCount prevents us from following long chain of resets (or some circular loop in redirects).
const maxResetRedirectCount = 100

// errNotDirectResetSuccessor rejects a direct-reset-successor candidate. It never leaves this package:
// recordOnDirectResetSuccessor replaces it with the original missing-run error.
var errNotDirectResetSuccessor = errors.New("current run is not an eligible direct reset successor of the recorded parent run")

// This API records the child completion event in the parent's history. It does the following.
//   - Rejects the request if the parent was closed for any reason other than reset.
//   - If the parent was closed due to reset, it forwards the request to the new parent following the resetRunID link.
//   - If the recorded parent run no longer exists (for example, retention deleted it after a reset), it delivers the
//     completion to the current run only when that run is a direct reset of the recorded run that inherited this
//     child already started (see isDirectResetSuccessor). Otherwise the original NotFound is returned unchanged.
//   - It ensures that the child sending the completion request was initialized by this parent before accepting the request.
func Invoke(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	// The run named by the child is the parent run that started it. Capture it before any ResetRunId
	// redirection below rewrites the request.
	recordedParentRunID := request.GetParentExecution().GetRunId()
	delivered := false
	defer func() {
		// The server's RetryableInterceptor invokes the handler again with this same request object after a
		// retryable error. Unless the completion was delivered, restore the recorded parent run ID so a retry
		// starts from the run the child named, not from a run reached through ResetRunId.
		if !delivered && request.GetParentExecution() != nil {
			request.ParentExecution.RunId = recordedParentRunID
		}
	}()

	_, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()), request.GetParentExecution().WorkflowId)
	if err != nil {
		return nil, err
	}

	// If the parent is reset, we need to follow a possible chain of resets to deliver the completion event to the correct parent.
	redirectCount := 0
	for {
		resetRunID, actionEntered, err := recordChildWorkflowCompleted(ctx, request, shardContext, workflowConsistencyChecker)
		if errors.Is(err, consts.ErrWorkflowCompleted) {
			// if the parent was reset, forward the request to the new run pointed by resetRunID
			// Note: An alternative solution is to load the current run here ane compare the originalRunIDs of the current run and the closed parent.
			// If they match, then deliver it to the current run. We should consider this optimization if we notice that reset chain is longer than 1-2 hops.
			if resetRunID != "" {
				if redirectCount >= maxResetRedirectCount {
					return nil, consts.ErrResetRedirectLimitReached
				}
				redirectCount++
				request.ParentExecution.RunId = resetRunID
				continue
			}
		}
		if err != nil {
			// Only the run named by the original request is eligible for the fallback, never a run reached
			// through ResetRunId.
			if redirectCount == 0 && isRecordedParentRunMissing(recordedParentRunID, actionEntered, err) {
				fallbackResp, fallbackErr := recordOnDirectResetSuccessor(ctx, request, recordedParentRunID, err, shardContext, workflowConsistencyChecker)
				delivered = fallbackErr == nil
				return fallbackResp, fallbackErr
			}
			return nil, err
		}
		delivered = true
		return &historyservice.RecordChildExecutionCompletedResponse{}, nil
	}
}

// isRecordedParentRunMissing reports whether loading the requested parent run failed because the run does not
// exist. The update action never ran, so the NotFound came from loading the run; ErrWorkflowCompleted and
// ErrChildExecutionNotFound are only returned from inside the action. NamespaceNotFound, retryable and shard
// ownership errors are not NotFound.
func isRecordedParentRunMissing(recordedParentRunID string, actionEntered bool, err error) bool {
	if recordedParentRunID == "" || actionEntered {
		return false
	}
	if errors.Is(err, consts.ErrWorkflowCompleted) || errors.Is(err, consts.ErrChildExecutionNotFound) {
		return false
	}
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// recordOnDirectResetSuccessor handles a completion whose recorded parent run no longer exists. It leases the
// current run of the parent workflow through the consistency checker and records the completion there only if
// isDirectResetSuccessor accepts it under that run's lock, before any mutation. In every other case it returns
// missingRunErr, the original error, so the caller sees exactly the outcome it saw before this fallback existed.
// Errors that are not NotFound (for example a retryable ErrLocateCurrentWorkflowExecution) are returned as is.
func recordOnDirectResetSuccessor(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	recordedParentRunID string,
	missingRunErr error,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.RecordChildExecutionCompletedResponse, error) {
	// An empty run ID makes the consistency checker lease the current run.
	currentRunKey := definition.NewWorkflowKey(request.GetNamespaceId(), request.GetParentExecution().GetWorkflowId(), "")
	_, _, err := recordChildCompletion(
		ctx,
		request,
		currentRunKey,
		func(mutableState historyi.MutableState) bool {
			return isDirectResetSuccessor(mutableState, request, recordedParentRunID)
		},
		shardContext,
		workflowConsistencyChecker,
	)
	switch {
	case err == nil:
		return &historyservice.RecordChildExecutionCompletedResponse{}, nil
	case errors.Is(err, errNotDirectResetSuccessor), common.IsNotFoundError(err):
		return nil, missingRunErr
	default:
		return nil, err
	}
}

// isDirectResetSuccessor reports whether the parent run in mutableState may record the completion of a child
// whose recorded parent run no longer exists. It accepts only a running run that was reset directly from the
// recorded parent run and that inherited this child's initiation and started identity through the reset:
//
//   - G1: the run is running.
//   - G2: the run was created by resetting the recorded parent run (BaseExecutionInfo is only written by the
//     workflow resetter).
//   - G3: the child initiation is at or before the reset point, so it is a copy of the recorded parent run's
//     initiation event; events written by the reset run itself have later event IDs.
//   - G4: the initiation event and the reset point are on the run's current branch. The version history is
//     checked directly, without the version-zero shortcut of api.IsHistoryEventOnCurrentBranch.
//   - G5: the request carries the child's first execution run ID.
//   - G6: the pending child at the initiation event has the initiation version and the requested workflow ID.
//   - G7: the pending child was started with the requesting child's first execution run ID. Children that are
//     only initiated are not supported.
func isDirectResetSuccessor(
	mutableState historyi.MutableState,
	request *historyservice.RecordChildExecutionCompletedRequest,
	recordedParentRunID string,
) bool {
	if !mutableState.IsWorkflowExecutionRunning() {
		return false
	}

	baseInfo := mutableState.GetBaseWorkflowInfo()
	if baseInfo == nil || baseInfo.GetRunId() == "" || baseInfo.GetRunId() != recordedParentRunID {
		return false
	}

	parentInitiatedID := request.GetParentInitiatedId()
	parentInitiatedVersion := request.GetParentInitiatedVersion()
	if parentInitiatedID < common.FirstEventID || parentInitiatedID > baseInfo.GetLowestCommonAncestorEventId() {
		return false
	}

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		return false
	}
	if !versionhistory.ContainsVersionHistoryItem(
		currentVersionHistory,
		versionhistory.NewVersionHistoryItem(parentInitiatedID, parentInitiatedVersion),
	) {
		return false
	}
	if !versionhistory.ContainsVersionHistoryItem(
		currentVersionHistory,
		versionhistory.NewVersionHistoryItem(baseInfo.GetLowestCommonAncestorEventId(), baseInfo.GetLowestCommonAncestorEventVersion()),
	) {
		return false
	}

	childFirstRunID := request.GetChildFirstExecutionRunId()
	if childFirstRunID == "" {
		return false
	}

	childInfo, ok := mutableState.GetChildExecutionInfo(parentInitiatedID)
	if !ok {
		return false
	}
	if childInfo.GetVersion() != parentInitiatedVersion ||
		childInfo.GetStartedWorkflowId() != request.GetChildExecution().GetWorkflowId() {
		return false
	}
	return childInfo.GetStartedEventId() != common.EmptyEventID && childInfo.GetStartedRunId() == childFirstRunID
}

// recordChildWorkflowCompleted records the child completed event in the parent history if the parent is still running.
// It returns consts.ErrWorkflowCompleted if the parent is already completed. Additionally a reset run ID is returned if the parent was completed due to a reset operation.
// actionEntered reports whether the parent run was loaded and the update action ran.
func recordChildWorkflowCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resetRunID string, actionEntered bool, retErr error) {
	return recordChildCompletion(
		ctx,
		request,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetParentExecution().WorkflowId,
			request.GetParentExecution().RunId,
		),
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
}

// recordChildCompletion records the child completion in the parent run identified by workflowKey (the current run
// when its run ID is empty). When eligible is not nil, it is evaluated first inside the update action, under the
// parent run's lock and before any mutation; a rejection returns errNotDirectResetSuccessor.
func recordChildCompletion(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	workflowKey definition.WorkflowKey,
	eligible func(historyi.MutableState) bool,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resetRunID string, actionEntered bool, retErr error) {
	parentInitiatedID := request.ParentInitiatedId
	parentInitiatedVersion := request.ParentInitiatedVersion
	retErr = api.GetAndUpdateWorkflowWithConsistencyCheck(
		ctx,
		request.Clock,
		func(mutableState historyi.MutableState) bool {
			if !mutableState.IsWorkflowExecutionRunning() {
				// current branch already closed, we won't perform any operation, pass the check
				return true
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil {
				// can't find initiated event, potential stale mutable, fail the predicate check
				return false
			}
			if !onCurrentBranch {
				// found on different branch, since we don't record completion on a different branch, pass the check
				return true
			}

			_, childInitEventFound := mutableState.GetChildExecutionInfo(parentInitiatedID)
			return childInitEventFound
		},
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			actionEntered = true
			mutableState := workflowLease.GetMutableState()
			if eligible != nil && !eligible(mutableState) {
				return nil, errNotDirectResetSuccessor
			}
			if !mutableState.IsWorkflowExecutionRunning() {
				resetRunID = mutableState.GetExecutionInfo().ResetRunId
				return nil, consts.ErrWorkflowCompleted
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil || !onCurrentBranch {
				return nil, consts.ErrChildExecutionNotFound
			}

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(parentInitiatedID)
			if !isRunning {
				return nil, consts.ErrChildExecutionNotFound
			}

			// note we already checked if startedEventID is empty (in consistency predicate)
			// and reloaded mutable state, so if startedEventID is still missing, we need to
			// record a started event before recording completion event.
			if err := recordStartedEventIfMissing(ctx, mutableState, request, ci); err != nil {
				return nil, err
			}

			childExecution := request.GetChildExecution()
			if ci.GetStartedWorkflowId() != childExecution.GetWorkflowId() {
				// this can only happen when we don't have the initiated version
				return nil, consts.ErrChildExecutionNotFound
			}

			if request.GetChildFirstExecutionRunId() != "" && ci.GetStartedRunId() != request.GetChildFirstExecutionRunId() {
				// this can happen when parent starts another child run in different branch
				return nil, consts.ErrChildExecutionNotFound
			}

			if err := addChildCompletionEvent(mutableState, parentInitiatedID, childExecution, request.CompletionEvent); err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	return resetRunID, actionEntered, retErr
}

// addChildCompletionEvent records the child completion event that matches the child's close event type.
func addChildCompletionEvent(
	mutableState historyi.MutableState,
	parentInitiatedID int64,
	childExecution *commonpb.WorkflowExecution,
	completionEvent *historypb.HistoryEvent,
) error {
	var err error
	switch completionEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attributes := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
		_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(parentInitiatedID, childExecution, attributes)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attributes := completionEvent.GetWorkflowExecutionFailedEventAttributes()
		_, err = mutableState.AddChildWorkflowExecutionFailedEvent(parentInitiatedID, childExecution, attributes)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		attributes := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
		_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(parentInitiatedID, childExecution, attributes)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(parentInitiatedID, childExecution)
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attributes := completionEvent.GetWorkflowExecutionTimedOutEventAttributes()
		_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(parentInitiatedID, childExecution, attributes)
	default:
		// Other event types do not close a child; nothing is recorded, as before.
	}
	return err
}

func recordStartedEventIfMissing(
	ctx context.Context,
	mutableState historyi.MutableState,
	request *historyservice.RecordChildExecutionCompletedRequest,
	ci *persistencespb.ChildExecutionInfo,
) error {
	parentInitiatedID := request.ParentInitiatedId
	if ci.StartedEventId == common.EmptyEventID {
		initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, parentInitiatedID)
		if err != nil {
			return consts.ErrChildExecutionNotFound
		}
		initiatedAttr := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		execution := &commonpb.WorkflowExecution{
			WorkflowId: request.GetChildExecution().GetWorkflowId(),
			RunId:      request.GetChildExecution().GetRunId(),
		}
		if request.GetChildFirstExecutionRunId() != "" {
			execution.RunId = request.GetChildFirstExecutionRunId()
		}
		// note values used here should not matter because the child info will be deleted
		// when the response is recorded, so it should be fine e.g. that ci.Clock is nil
		_, err = mutableState.AddChildWorkflowExecutionStartedEvent(
			execution,
			initiatedAttr.WorkflowType,
			initiatedEvent.EventId,
			initiatedAttr.Header,
			ci.Clock,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
