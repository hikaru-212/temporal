package recordchildworkflowcompleted

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// tests that the child execution completed request is forwarded to the new parent in case of resets.
func Test_Recordchildworkflowcompleted_WithForwards(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFID := uuid.NewString()
	paretntWFID := uuid.NewString()
	oldParentRunID := uuid.NewString()
	newParentRunID := uuid.NewString()
	oldParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, oldParentRunID)
	newParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, newParentRunID)
	oldParentExecutionInfo := &persistencespb.WorkflowExecutionInfo{
		ResetRunId: newParentRunID, // link the old parent to the new parent.
	}

	// The request will be sent to the old parent.
	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			RunId:      oldParentRunID,
			WorkflowId: paretntWFID,
		},
		ChildExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mockRegistery := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(testNamespace, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := historyi.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo)

	newParentMutableState := historyi.NewMockMutableState(ctrl)
	newParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	newParentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	newParentMutableState.EXPECT().GetNextEventID().Return(int64(10))
	newParentMutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(anyArg, anyArg, anyArg).Return(nil, nil)
	childExecutionInfo := &persistencespb.ChildExecutionInfo{
		StartedEventId:    int64(10), // indicate that the started event is already recorded.
		StartedWorkflowId: childWFID,
	}
	newParentMutableState.EXPECT().GetChildExecutionInfo(anyArg).Return(childExecutionInfo, true)
	newParentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	newParentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(anyArg, anyArg).Return(nil, nil)

	mockWFContext := historyi.NewMockWorkflowContext(ctrl)
	mockWFContext.EXPECT().UpdateWorkflowExecutionAsActive(anyArg, anyArg).Return(nil)

	oldParentWFLease := ndc.NewMockWorkflow(ctrl)
	oldParentWFLease.EXPECT().GetMutableState().Return(oldParentMutableState) // old parent's mutable state is accesses just once.
	oldParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease := ndc.NewMockWorkflow(ctrl)
	newParentWFLease.EXPECT().GetMutableState().Return(newParentMutableState).AnyTimes() // new parent's mutable state would be accessed many times.
	newParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease.EXPECT().GetContext().Return(mockWFContext)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, oldParentWFKey, anyArg).Return(oldParentWFLease, nil)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, newParentWFKey, anyArg).Return(newParentWFLease, nil)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, newParentRunID, request.ParentExecution.RunId) // the request should be modified to point to the new parent.
}

// tests that we break out of the loop after max redirect attempts.
func Test_Recordchildworkflowcompleted_WithInfiniteForwards(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFID := uuid.NewString()
	paretntWFID := uuid.NewString()
	oldParentRunID := uuid.NewString()
	oldParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, oldParentRunID)
	oldParentExecutionInfo := &persistencespb.WorkflowExecutionInfo{
		ResetRunId: oldParentRunID, // link to self causing an infinite loop.
	}

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			RunId:      oldParentRunID,
			WorkflowId: paretntWFID,
		},
		ChildExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mockRegistery := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(testNamespace, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := historyi.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(maxResetRedirectCount + 1)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo).Times(maxResetRedirectCount + 1)

	oldParentWFLease := ndc.NewMockWorkflow(ctrl)
	oldParentWFLease.EXPECT().GetMutableState().Return(oldParentMutableState).Times(maxResetRedirectCount + 1)
	oldParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {}).Times(maxResetRedirectCount + 1)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, oldParentWFKey, anyArg).Return(oldParentWFLease, nil).Times(maxResetRedirectCount + 1)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.ErrorIs(t, err, consts.ErrResetRedirectLimitReached)
	require.Nil(t, resp)
}

// fallbackHarness wires the mocks shared by the tests of the direct-reset-successor fallback, which applies
// when the recorded parent run no longer exists.
type fallbackHarness struct {
	ctrl               *gomock.Controller
	shardContext       *historyi.MockShardContext
	consistencyChecker *api.MockWorkflowConsistencyChecker
	namespaceID        string
	parentWorkflowID   string
	recordedRunID      string
	childWorkflowID    string
	childFirstRunID    string
	missingRunErr      error
}

func newFallbackHarness(t *testing.T) *fallbackHarness {
	ctrl := gomock.NewController(t)
	testNamespaceID := tests.NamespaceID

	mockRegistry := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistry.EXPECT().GetNamespaceByID(testNamespaceID).Return(testNamespace, nil).AnyTimes()
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("").AnyTimes()
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata).AnyTimes()

	parentWorkflowID := uuid.NewString()
	recordedRunID := uuid.NewString()
	return &fallbackHarness{
		ctrl:               ctrl,
		shardContext:       shardContext,
		consistencyChecker: api.NewMockWorkflowConsistencyChecker(ctrl),
		namespaceID:        testNamespaceID.String(),
		parentWorkflowID:   parentWorkflowID,
		recordedRunID:      recordedRunID,
		childWorkflowID:    uuid.NewString(),
		childFirstRunID:    uuid.NewString(),
		// The persistence layer's NotFound for a missing run, returned while loading the recorded run.
		missingRunErr: serviceerror.NewNotFoundf(
			"workflow execution not found for workflow ID %q and run ID %q", parentWorkflowID, recordedRunID),
	}
}

func (h *fallbackHarness) recordedRunKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(h.namespaceID, h.parentWorkflowID, h.recordedRunID)
}

func (h *fallbackHarness) currentRunKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(h.namespaceID, h.parentWorkflowID, "")
}

func (h *fallbackHarness) request(initiatedID, initiatedVersion int64, childFirstRunID string) *historyservice.RecordChildExecutionCompletedRequest {
	return &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: h.namespaceID,
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: h.parentWorkflowID,
			RunId:      h.recordedRunID,
		},
		ParentInitiatedId:        initiatedID,
		ParentInitiatedVersion:   initiatedVersion,
		ChildExecution:           &commonpb.WorkflowExecution{WorkflowId: h.childWorkflowID, RunId: uuid.NewString()},
		ChildFirstExecutionRunId: childFirstRunID,
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{},
			},
		},
	}
}

// expectRecordedRunMissing makes loading the recorded parent run fail before the update action runs.
func (h *fallbackHarness) expectRecordedRunMissing() {
	h.consistencyChecker.EXPECT().
		GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
		Return(nil, h.missingRunErr)
}

// lease returns a workflow lease for mutableState. A context is only provided when the test expects a commit;
// otherwise any persistence attempt fails the test.
func (h *fallbackHarness) lease(mutableState historyi.MutableState, wfContext historyi.WorkflowContext) *ndc.MockWorkflow {
	lease := ndc.NewMockWorkflow(h.ctrl)
	lease.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	lease.EXPECT().GetReleaseFn().Return(func(error) {})
	if wfContext != nil {
		lease.EXPECT().GetContext().Return(wfContext)
	}
	return lease
}

// candidateState describes the current run the fallback considers.
type candidateState struct {
	running             bool
	base                *workflowspb.BaseExecutionInfo
	versionHistoryItems []*historyspb.VersionHistoryItem
	childInfo           *persistencespb.ChildExecutionInfo
}

func (h *fallbackHarness) candidate(state candidateState) *historyi.MockMutableState {
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{{
				BranchToken: []byte("candidate-branch"),
				Items:       state.versionHistoryItems,
			}},
		},
	}
	mutableState := historyi.NewMockMutableState(h.ctrl)
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(state.running).AnyTimes()
	mutableState.EXPECT().GetBaseWorkflowInfo().Return(state.base).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(int64(100)).AnyTimes()
	mutableState.EXPECT().GetChildExecutionInfo(gomock.Any()).DoAndReturn(
		func(initiatedID int64) (*persistencespb.ChildExecutionInfo, bool) {
			if state.childInfo == nil || state.childInfo.GetInitiatedEventId() != initiatedID {
				return nil, false
			}
			return state.childInfo, true
		}).AnyTimes()
	return mutableState
}

// fallbackScenario is an eligible direct reset successor; test cases break one guard at a time.
type fallbackScenario struct {
	initiatedID      int64
	initiatedVersion int64
	childFirstRunID  string
	candidate        candidateState
}

func (h *fallbackHarness) eligibleScenario() fallbackScenario {
	return fallbackScenario{
		initiatedID:      5,
		initiatedVersion: 0,
		childFirstRunID:  h.childFirstRunID,
		candidate: candidateState{
			running: true,
			base: &workflowspb.BaseExecutionInfo{
				RunId:                            h.recordedRunID,
				LowestCommonAncestorEventId:      8,
				LowestCommonAncestorEventVersion: 0,
			},
			versionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 20, Version: 0}},
			childInfo: &persistencespb.ChildExecutionInfo{
				InitiatedEventId:  5,
				Version:           0,
				StartedEventId:    6,
				StartedWorkflowId: h.childWorkflowID,
				StartedRunId:      h.childFirstRunID,
			},
		},
	}
}

// Test_Recordchildworkflowcompleted_DirectResetSuccessorFallback covers the fallback taken when the recorded parent
// run no longer exists: the completion is recorded on the current run only if it is a direct reset of the recorded
// run that inherited the started child; otherwise the original NotFound is returned and nothing is persisted.
func Test_Recordchildworkflowcompleted_DirectResetSuccessorFallback(t *testing.T) {
	testCases := []struct {
		name      string
		mutate    func(h *fallbackHarness, s *fallbackScenario)
		delivered bool
	}{
		{
			name:      "eligible direct reset successor at version zero",
			mutate:    func(*fallbackHarness, *fallbackScenario) {},
			delivered: true,
		},
		{
			name: "eligible direct reset successor at a nonzero version",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.initiatedVersion = 100
				s.candidate.base.LowestCommonAncestorEventVersion = 100
				s.candidate.versionHistoryItems = []*historyspb.VersionHistoryItem{{EventId: 20, Version: 100}}
				s.candidate.childInfo.Version = 100
			},
			delivered: true,
		},
		{
			name: "G1: current run is closed",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.running = false
			},
		},
		{
			name: "G2: unrelated current run without base execution info",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.base = nil
			},
		},
		{
			name: "G2: current run was reset from a different run",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.base.RunId = uuid.NewString()
			},
		},
		{
			name: "G3: child initiated after the reset point",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.initiatedID = 10
				s.candidate.childInfo.InitiatedEventId = 10
			},
		},
		{
			name: "G4: initiation version is not on the current branch",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.initiatedVersion = 100
				s.candidate.childInfo.Version = 100
			},
		},
		{
			name: "G4: reset point version is not on the current branch",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.base.LowestCommonAncestorEventVersion = 200
			},
		},
		{
			name: "G4: reset point beyond the current version-zero history",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.base.LowestCommonAncestorEventId = 30
			},
		},
		{
			name: "G5: empty child first execution run ID",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.childFirstRunID = ""
			},
		},
		{
			name: "G6: no pending child at the initiation event",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.childInfo = nil
			},
		},
		{
			name: "G6: pending child has a different initiation version",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.childInfo.Version = 7
			},
		},
		{
			name: "G6: pending child has a different workflow ID",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.childInfo.StartedWorkflowId = uuid.NewString()
			},
		},
		{
			name: "G7: pending child is only initiated",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.candidate.childInfo.StartedEventId = common.EmptyEventID
				s.candidate.childInfo.StartedRunId = ""
			},
		},
		{
			name: "G7: pending child was started with a different first run ID",
			mutate: func(_ *fallbackHarness, s *fallbackScenario) {
				s.childFirstRunID = uuid.NewString()
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newFallbackHarness(t)
			scenario := h.eligibleScenario()
			tc.mutate(h, &scenario)
			request := h.request(scenario.initiatedID, scenario.initiatedVersion, scenario.childFirstRunID)
			childExecution := request.GetChildExecution()

			h.expectRecordedRunMissing()
			candidate := h.candidate(scenario.candidate)
			// Only a delivered completion may mutate and persist the candidate. For rejected cases no context is
			// provided and none of the mutating calls are expected, so any attempt fails the test.
			var wfContext historyi.WorkflowContext
			if tc.delivered {
				candidate.EXPECT().
					AddChildWorkflowExecutionCompletedEvent(scenario.initiatedID, childExecution, gomock.Any()).
					Return(&historypb.HistoryEvent{}, nil)
				candidate.EXPECT().HasPendingWorkflowTask().Return(false)
				candidate.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
				candidate.EXPECT().AddWorkflowTaskScheduledEvent(false, gomock.Any()).Return(nil, nil)
				mockContext := historyi.NewMockWorkflowContext(h.ctrl)
				mockContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil)
				wfContext = mockContext
			}
			h.consistencyChecker.EXPECT().
				GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.currentRunKey(), gomock.Any()).
				Return(h.lease(candidate, wfContext), nil)

			resp, err := Invoke(context.Background(), request, h.shardContext, h.consistencyChecker)
			if tc.delivered {
				require.NoError(t, err)
				require.NotNil(t, resp)
			} else {
				require.ErrorIs(t, err, h.missingRunErr, "a rejected fallback must return the original missing-run error")
				require.Nil(t, resp)
			}
			// The request still names the recorded parent run.
			require.Equal(t, h.recordedRunID, request.GetParentExecution().GetRunId())
		})
	}
}

// Test_Recordchildworkflowcompleted_FallbackCurrentRunErrors covers failures to lease the current run.
func Test_Recordchildworkflowcompleted_FallbackCurrentRunErrors(t *testing.T) {
	t.Run("retryable current-run error is returned as is", func(t *testing.T) {
		h := newFallbackHarness(t)
		h.expectRecordedRunMissing()
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.currentRunKey(), gomock.Any()).
			Return(nil, consts.ErrLocateCurrentWorkflowExecution)

		resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, consts.ErrLocateCurrentWorkflowExecution)
		require.Nil(t, resp)
	})

	t.Run("no current run keeps the original missing-run error", func(t *testing.T) {
		h := newFallbackHarness(t)
		h.expectRecordedRunMissing()
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.currentRunKey(), gomock.Any()).
			Return(nil, serviceerror.NewNotFound("current workflow execution not found"))

		resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, h.missingRunErr)
		require.Nil(t, resp)
	})
}

// Test_Recordchildworkflowcompleted_NoFallback covers errors that must not trigger the fallback. The mock
// consistency checker has no expectation for the current-run key, so any fallback attempt fails the test.
func Test_Recordchildworkflowcompleted_NoFallback(t *testing.T) {
	t.Run("recorded parent closed without reset returns ErrWorkflowCompleted", func(t *testing.T) {
		h := newFallbackHarness(t)
		recordedRun := historyi.NewMockMutableState(h.ctrl)
		recordedRun.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
		recordedRun.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(h.lease(recordedRun, nil), nil)

		resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, consts.ErrWorkflowCompleted)
		require.Nil(t, resp)
	})

	t.Run("recorded parent without the pending child returns ErrChildExecutionNotFound", func(t *testing.T) {
		h := newFallbackHarness(t)
		recordedRun := historyi.NewMockMutableState(h.ctrl)
		recordedRun.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
		recordedRun.EXPECT().GetNextEventID().Return(int64(100)).AnyTimes()
		recordedRun.EXPECT().GetChildExecutionInfo(int64(5)).Return(nil, false).AnyTimes()
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(h.lease(recordedRun, nil), nil)

		resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, consts.ErrChildExecutionNotFound)
		require.Nil(t, resp)
	})

	for _, loadErr := range []error{
		serviceerror.NewNamespaceNotFound("namespace"),
		serviceerror.NewUnavailable("unavailable"),
		consts.ErrLocateCurrentWorkflowExecution,
	} {
		t.Run("load error "+loadErr.Error()+" is returned as is", func(t *testing.T) {
			h := newFallbackHarness(t)
			h.consistencyChecker.EXPECT().
				GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
				Return(nil, loadErr)

			resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
			require.ErrorIs(t, err, loadErr)
			require.Nil(t, resp)
		})
	}

	t.Run("missing run reached through ResetRunId is not a fallback trigger", func(t *testing.T) {
		h := newFallbackHarness(t)
		resetRunID := uuid.NewString()
		recordedRun := historyi.NewMockMutableState(h.ctrl)
		recordedRun.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
		recordedRun.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{ResetRunId: resetRunID}).AnyTimes()
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(h.lease(recordedRun, nil), nil)
		redirectedMissingErr := serviceerror.NewNotFound("reset run not found")
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(),
				definition.NewWorkflowKey(h.namespaceID, h.parentWorkflowID, resetRunID), gomock.Any()).
			Return(nil, redirectedMissingErr)

		resp, err := Invoke(context.Background(), h.request(5, 0, h.childFirstRunID), h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, redirectedMissingErr)
		require.Nil(t, resp)
	})

	t.Run("generic NotFound returned from inside the update action is not a fallback trigger", func(t *testing.T) {
		h := newFallbackHarness(t)
		request := h.request(5, 0, h.childFirstRunID)
		actionErr := serviceerror.NewNotFound("not found inside the update action")
		recordedRun := historyi.NewMockMutableState(h.ctrl)
		recordedRun.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
		recordedRun.EXPECT().GetNextEventID().Return(int64(100)).AnyTimes()
		recordedRun.EXPECT().GetChildExecutionInfo(int64(5)).Return(&persistencespb.ChildExecutionInfo{
			InitiatedEventId:  5,
			StartedEventId:    6,
			StartedWorkflowId: h.childWorkflowID,
			StartedRunId:      h.childFirstRunID,
		}, true).AnyTimes()
		recordedRun.EXPECT().
			AddChildWorkflowExecutionCompletedEvent(int64(5), request.GetChildExecution(), gomock.Any()).
			Return(nil, actionErr)
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(h.lease(recordedRun, nil), nil)

		resp, err := Invoke(context.Background(), request, h.shardContext, h.consistencyChecker)
		require.ErrorIs(t, err, actionErr)
		require.Nil(t, resp)
	})
}

// Test_Recordchildworkflowcompleted_FallbackCandidatePersistenceFailure checks that a failed commit on an eligible
// candidate is returned as an error, not reported as a delivered completion.
func Test_Recordchildworkflowcompleted_FallbackCandidatePersistenceFailure(t *testing.T) {
	h := newFallbackHarness(t)
	scenario := h.eligibleScenario()
	request := h.request(scenario.initiatedID, scenario.initiatedVersion, scenario.childFirstRunID)
	persistErr := serviceerror.NewUnavailable("candidate update failed")

	h.expectRecordedRunMissing()
	candidate := h.candidate(scenario.candidate)
	candidate.EXPECT().
		AddChildWorkflowExecutionCompletedEvent(scenario.initiatedID, request.GetChildExecution(), gomock.Any()).
		Return(&historypb.HistoryEvent{}, nil)
	candidate.EXPECT().HasPendingWorkflowTask().Return(false)
	candidate.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	candidate.EXPECT().AddWorkflowTaskScheduledEvent(false, gomock.Any()).Return(nil, nil)
	wfContext := historyi.NewMockWorkflowContext(h.ctrl)
	wfContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(persistErr)
	h.consistencyChecker.EXPECT().
		GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.currentRunKey(), gomock.Any()).
		Return(h.lease(candidate, wfContext), nil)

	resp, err := Invoke(context.Background(), request, h.shardContext, h.consistencyChecker)
	require.ErrorIs(t, err, persistErr)
	require.Nil(t, resp)
	require.Equal(t, h.recordedRunID, request.GetParentExecution().GetRunId())
}

// Test_Recordchildworkflowcompleted_RetrySameRequestRestoresRecordedParent reproduces a server-side retry: the
// RetryableInterceptor invokes the handler again with the same request object. A failed invocation that followed
// ResetRunId must not leave the redirected run in the request, otherwise the retry would treat that run as the
// child's recorded parent and could take the single-hop fallback from it.
func Test_Recordchildworkflowcompleted_RetrySameRequestRestoresRecordedParent(t *testing.T) {
	h := newFallbackHarness(t)
	resetRunID := uuid.NewString()
	request := h.request(5, 0, h.childFirstRunID)

	// R1 is closed by a reset to R2 in both invocations.
	recordedRun := historyi.NewMockMutableState(h.ctrl)
	recordedRun.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	recordedRun.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{ResetRunId: resetRunID}).AnyTimes()
	recordedLease := ndc.NewMockWorkflow(h.ctrl)
	recordedLease.EXPECT().GetMutableState().Return(recordedRun).AnyTimes()
	recordedLease.EXPECT().GetReleaseFn().Return(func(error) {}).Times(2)

	resetRunKey := definition.NewWorkflowKey(h.namespaceID, h.parentWorkflowID, resetRunID)
	retryableErr := serviceerror.NewUnavailable("reset run temporarily unavailable")
	resetRunMissingErr := serviceerror.NewNotFound("reset run not found")
	// The current-run key has no expectation: any fallback attempt fails the test.
	gomock.InOrder(
		// First invocation: R1 redirects to R2, and R2 fails with a retryable error.
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(recordedLease, nil),
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), resetRunKey, gomock.Any()).
			Return(nil, retryableErr),
		// Second invocation with the same request object: it must start from R1 again. R2 is now missing, but it
		// was reached through ResetRunId, so it is not a fallback trigger.
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), h.recordedRunKey(), gomock.Any()).
			Return(recordedLease, nil),
		h.consistencyChecker.EXPECT().
			GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), gomock.Any(), gomock.Any(), resetRunKey, gomock.Any()).
			Return(nil, resetRunMissingErr),
	)

	resp, err := Invoke(context.Background(), request, h.shardContext, h.consistencyChecker)
	require.ErrorIs(t, err, retryableErr)
	require.Nil(t, resp)
	require.Equal(t, h.recordedRunID, request.GetParentExecution().GetRunId(), "a failed invocation must restore the recorded parent run")

	resp, err = Invoke(context.Background(), request, h.shardContext, h.consistencyChecker)
	require.ErrorIs(t, err, resetRunMissingErr)
	require.Nil(t, resp)
	require.Equal(t, h.recordedRunID, request.GetParentExecution().GetRunId(), "a failed invocation must restore the recorded parent run")
}
