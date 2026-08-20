use crate::StartNodeCommand;
use crate::StartTeamCommand;
use crate::TeamControl;
use crate::TeamRuntimeError;
use crate::TransitionCommand;
use crate::control::EndTeamCommand;
use crate::control::RecordResultCommand;
use crate::ids::StateRevision;
use crate::ids::TeamSessionId;
use crate::tests_support::sample_graph;
use codex_team_graph::TeamGraphCatalog;
use pretty_assertions::assert_eq;

fn control() -> TeamControl {
    TeamControl::memory(TeamGraphCatalog::new([sample_graph()]))
}

#[tokio::test]
async fn bind_agent_emits_backend_fallback_only_when_marked() {
    let sink = crate::RecordingSink::default();
    let control =
        TeamControl::with_memory_store(TeamGraphCatalog::new([sample_graph()]), sink.clone());
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let mut pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    control
        .bind_agent_before_start("agent-normal", pending.clone())
        .await
        .expect("normal bind");
    pending.backend_fallback = true;
    control
        .bind_agent_before_start("agent-fallback", pending)
        .await
        .expect("fallback bind");
    let attached: Vec<_> = sink
        .envelopes()
        .into_iter()
        .filter(|envelope| envelope.kind == "agent_attached")
        .collect();
    assert_eq!(attached.len(), 2);
    assert!(attached[0].payload.get("backend_fallback").is_none());
    assert_eq!(attached[1].payload["backend_fallback"], true);
}

#[tokio::test]
async fn bind_agent_emits_resolved_attach_metadata_without_persisting_it_in_binding() {
    let sink = crate::RecordingSink::default();
    let control =
        TeamControl::with_memory_store(TeamGraphCatalog::new([sample_graph()]), sink.clone());
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let exact_message = "delegate\nwith  spacing and 日本語";
    let mut pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    pending.attach_metadata = Some(crate::PendingAgentAttachMetadata {
        delegation_message: exact_message.into(),
        identity: Some(crate::AgentBackendIdentity::Acp {
            harness: "cursor".into(),
            model: Some("cursor-model".into()),
        }),
    });
    assert!(
        serde_json::to_value(&pending)
            .expect("serialize pending binding")
            .get("attach_metadata")
            .is_none(),
        "attach metadata must persist only through agent_attached"
    );
    let binding = control
        .bind_agent_before_start("agent-acp", pending)
        .await
        .expect("bind");

    let envelope = sink
        .envelopes()
        .into_iter()
        .find(|event| event.kind == "agent_attached")
        .expect("agent_attached");
    assert_eq!(envelope.payload["backend"], "acp");
    assert_eq!(envelope.payload["harness"], "cursor");
    assert_eq!(envelope.payload["model"], "cursor-model");
    assert_eq!(envelope.payload["delegation_message"], exact_message);
    assert!(binding.to_pending().attach_metadata.is_none());
}

#[tokio::test]
async fn bind_agent_rejects_unresolved_attach_metadata_before_persistence() {
    let sink = crate::RecordingSink::default();
    let control =
        TeamControl::with_memory_store(TeamGraphCatalog::new([sample_graph()]), sink.clone());
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let mut pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    pending.attach_metadata = Some(crate::PendingAgentAttachMetadata::new("delegate".into()));

    let error = control
        .bind_agent_before_start("untraced-agent", pending)
        .await
        .expect_err("unresolved identity must fail before binding");
    assert!(matches!(error, TeamRuntimeError::Invalid(_)));
    assert!(control.binding_for("untraced-agent").await.is_none());
    assert!(
        sink.envelopes()
            .into_iter()
            .all(|event| event.kind != "agent_attached")
    );
}

#[tokio::test]
async fn two_teams_do_not_share_revision_or_node() {
    let control = control();
    let a = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("a".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("a");
    let b = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("b".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("b");
    assert_ne!(a.team_session_id, b.team_session_id);
    let a_node = control
        .start_node(StartNodeCommand {
            team_session_id: a.team_session_id.clone(),
            node_id: None,
            expected_revision: a.revision,
        })
        .await
        .expect("a node");
    let b_status = control.status(&b.team_session_id).await.expect("b status");
    assert_eq!(b_status.revision, b.revision);
    assert!(b_status.current_node.is_some());
    assert_eq!(a_node.revision.get(), a.revision.get() + 1);
}

#[tokio::test]
async fn rejects_cross_team_agent_reference() {
    let control = control();
    let a = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("a");
    let b = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("b");
    control
        .start_node(StartNodeCommand {
            team_session_id: a.team_session_id.clone(),
            node_id: None,
            expected_revision: a.revision,
        })
        .await
        .expect("node");
    let pending = control
        .pending_binding_for_node(&a.team_session_id, "worker")
        .await
        .expect("pending");
    control
        .bind_agent_before_start("agent-a", pending)
        .await
        .expect("bind");
    let err = control
        .require_same_team(&b.team_session_id, "agent-a")
        .await
        .expect_err("cross team");
    assert!(matches!(err, TeamRuntimeError::CrossTeamRef { .. }));
}

#[tokio::test]
async fn stale_revision_cas_rejects_transition() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let recorded = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            evidence_id: Some("ev_work".into()),
            candidate_sha: Some("0123456789abcdef0123456789abcdef01234567".into()),
            qa: None,
            findings: None,
            expected_revision: node.revision,
        })
        .await
        .expect("result");
    assert_eq!(
        recorded.candidate_sha.as_deref(),
        Some("0123456789abcdef0123456789abcdef01234567")
    );
    let err = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: StateRevision::new(1),
        })
        .await
        .expect_err("stale");
    assert!(matches!(err, TeamRuntimeError::StaleRevision { .. }));
}

#[tokio::test]
async fn sequences_are_per_team() {
    let control = control();
    let a = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("a");
    let b = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("b");
    let a2 = control
        .start_node(StartNodeCommand {
            team_session_id: a.team_session_id.clone(),
            node_id: None,
            expected_revision: a.revision,
        })
        .await
        .expect("a node");
    let b2 = control
        .start_node(StartNodeCommand {
            team_session_id: b.team_session_id.clone(),
            node_id: None,
            expected_revision: b.revision,
        })
        .await
        .expect("b node");
    assert_eq!(a2.revision.get(), 2);
    assert_eq!(b2.revision.get(), 2);
}

#[tokio::test]
async fn unknown_team_is_not_found() {
    let control = control();
    let err = control
        .status(&TeamSessionId::generate())
        .await
        .expect_err("missing");
    assert!(matches!(err, TeamRuntimeError::TeamNotFound(_)));
}

#[tokio::test]
async fn end_team_closes_session() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("start node");
    let recorded = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: None,
            expected_revision: node.revision,
        })
        .await
        .expect("record result");
    let transitioned = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: recorded.revision,
        })
        .await
        .expect("transition to terminal");
    let ended = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: transitioned.revision,
        })
        .await
        .expect("end");
    assert!(!control.has_open_teams().await);
    assert!(ended.possible_next.is_empty());
}

#[tokio::test]
async fn fake_acp_lifecycle_records_pool_retry_and_unreported_coverage() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    control
        .bind_agent_before_start("acp-1", pending.clone())
        .await
        .expect("first backend");
    control
        .record_agent_terminal("acp-1", "errored")
        .await
        .expect("first terminal");
    let mut fallback = pending;
    fallback.backend_fallback = true;
    control
        .bind_agent_before_start("acp-2", fallback)
        .await
        .expect("retry backend");
    control
        .record_tool_operation(
            "acp-2",
            "unknown",
            "call-1",
            crate::TeamEventKind::ToolCoverageUnreported,
            Some("unreported"),
        )
        .await
        .expect("coverage");
    control
        .record_agent_terminal("acp-2", "completed")
        .await
        .expect("second terminal");
    let binding = control.binding_snapshot("acp-2");
    assert!(binding.is_some());
}

#[tokio::test]
async fn pending_binding_rejects_role_mismatch() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let err = control
        .pending_binding_for_node(&started.team_session_id, "textil_worker_default")
        .await
        .expect_err("role mismatch");
    assert!(matches!(err, TeamRuntimeError::RoleMismatch { .. }));
}

#[tokio::test]
async fn closed_team_rejects_lifecycle_and_unstarted_node_completion() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let err = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: None,
            expected_revision: started.revision,
        })
        .await
        .expect_err("unstarted node");
    assert!(matches!(err, TeamRuntimeError::NoActiveNodeRun(_)));

    let ended = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: true,
            reason: "aborted".into(),
            expected_revision: started.revision,
        })
        .await
        .expect("end");
    let err = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: ended.revision,
        })
        .await
        .expect_err("closed");
    assert!(matches!(err, TeamRuntimeError::ClosedTeam(_)));
}

#[tokio::test]
async fn rejects_double_start_while_node_run_is_active() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("start node");

    // Second start on active uncompleted run must be rejected.
    let err = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: node.revision,
        })
        .await
        .expect_err("double start must fail");
    assert!(matches!(err, TeamRuntimeError::ActiveNodeRunExists(_)));
}

#[tokio::test]
async fn rejects_transition_before_node_result_completion() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");

    // Transition before node started: rejected.
    let err = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: started.revision,
        })
        .await
        .expect_err("transition before node start");
    assert!(matches!(err, TeamRuntimeError::NoCompletedNodeRun(_)));

    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");

    // Transition before record_result: rejected.
    let err = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: node.revision,
        })
        .await
        .expect_err("transition before record_result");
    assert!(matches!(err, TeamRuntimeError::NoCompletedNodeRun(_)));

    // After record_result, transition with matching result succeeds.
    let recorded = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: None,
            expected_revision: node.revision,
        })
        .await
        .expect("record_result");
    let transitioned = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: recorded.revision,
        })
        .await
        .expect("transition");
    assert_eq!(
        transitioned.current_node.unwrap().node_id.as_str(),
        "completed"
    );
}

#[tokio::test]
async fn rejects_normal_end_with_active_run_or_agent_and_allows_abort() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");

    // 1. Non-terminal node end_team(aborted: false) rejected.
    let err = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: started.revision,
        })
        .await
        .expect_err("non-terminal end rejected");
    assert!(matches!(err, TeamRuntimeError::NonTerminalNode { .. }));

    // Move to terminal node "completed".
    let _node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending agent binding");
    control
        .bind_agent_before_start("active-worker", pending)
        .await
        .expect("bind active agent");
    let agent_attached = control
        .status(&started.team_session_id)
        .await
        .expect("status after bind");
    let recorded = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: None,
            expected_revision: agent_attached.revision,
        })
        .await
        .expect("result");
    let transitioned = control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "candidate_ready".into(),
            deviation_reason: None,
            expected_revision: recorded.revision,
        })
        .await
        .expect("transition");

    // 2. Terminal node with active run: start_node on terminal node without record_result.
    let terminal_run = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: transitioned.revision,
        })
        .await
        .expect("terminal start_node");
    let err = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: terminal_run.revision,
        })
        .await
        .expect_err("active run end rejected");
    assert!(matches!(err, TeamRuntimeError::ActiveNodeRunExists(_)));

    // Complete the terminal run while leaving the bound agent active.
    let terminal_completed = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "done".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: None,
            expected_revision: terminal_run.revision,
        })
        .await
        .expect("terminal result");

    let err = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: terminal_completed.revision,
        })
        .await
        .expect_err("active agent normal end rejected");
    assert!(matches!(err, TeamRuntimeError::ActiveAgents(_)));

    let ended = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: true,
            reason: "abort with active agent".into(),
            expected_revision: terminal_completed.revision,
        })
        .await
        .expect("active agent aborted end allowed");
    assert_eq!(ended.lifecycle, crate::state::TeamLifecycle::Aborted);
}

#[tokio::test]
async fn allows_aborted_end_team_mid_run() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    // Mid-run on non-terminal node with active run: aborted=true is allowed.
    let aborted = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: true,
            reason: "user requested abort".into(),
            expected_revision: node.revision,
        })
        .await
        .expect("abort");
    assert_eq!(aborted.lifecycle, crate::state::TeamLifecycle::Aborted);
}

#[tokio::test]
async fn agent_wait_entered_and_resolved_preserves_agent_lifecycle() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");

    let waited = control
        .record_agent_wait_entered(&started.team_session_id, "agent-worker", "worker result")
        .await
        .expect("wait entered");
    assert_eq!(waited.lifecycle, crate::state::TeamLifecycle::WaitingAgent);
    assert_eq!(waited.waiting_reason, None);

    let resolved = control
        .record_agent_wait_resolved(&started.team_session_id, "agent-worker", "worker result")
        .await
        .expect("wait resolved");
    assert_eq!(resolved.lifecycle, crate::state::TeamLifecycle::Running);
    assert_eq!(resolved.waiting_reason, None);
}

#[tokio::test]
async fn wait_and_evidence_reach_the_append_only_trace() {
    let control = control();
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let waited = control
        .enter_external_wait(crate::ExternalWaitCommand {
            team_session_id: started.team_session_id.clone(),
            reason: "ci".into(),
            expected_revision: started.revision,
        })
        .await
        .expect("wait");
    assert_eq!(waited.waiting_reason.as_deref(), Some("ci"));
    let resolved = control
        .resolve_external_wait(crate::ExternalWaitCommand {
            team_session_id: started.team_session_id.clone(),
            reason: "ci".into(),
            expected_revision: waited.revision,
        })
        .await
        .expect("resolve");
    assert_eq!(resolved.waiting_reason, None);

    let recorded = control
        .record_evidence(
            crate::EvidenceCommand {
                team_session_id: started.team_session_id.clone(),
                evidence_id: "ev_qa".into(),
                identity: Some("sha".into()),
                expected_revision: resolved.revision,
            },
            crate::TeamEventKind::EvidenceRecorded,
        )
        .await
        .expect("evidence");
    control
        .record_evidence(
            crate::EvidenceCommand {
                team_session_id: started.team_session_id.clone(),
                evidence_id: "ev_qa".into(),
                identity: None,
                expected_revision: recorded.revision,
            },
            crate::TeamEventKind::EvidenceInvalidated,
        )
        .await
        .expect("invalidate");
}

#[tokio::test]
async fn transition_copies_graph_metric_effects_and_ignores_caller_injection() {
    let sink = crate::RecordingSink::default();
    let control = TeamControl::with_memory_store(
        TeamGraphCatalog::new([crate::tests_support::review_return_graph()]),
        sink.clone(),
    );
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "review-return".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let node = control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let recorded = control
        .record_result(RecordResultCommand {
            team_session_id: started.team_session_id.clone(),
            result: "changes_requested".into(),
            evidence_id: None,
            candidate_sha: None,
            qa: None,
            findings: Some(2),
            expected_revision: node.revision,
        })
        .await
        .expect("result");
    control
        .transition(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "changes_requested".into(),
            deviation_reason: None,
            expected_revision: recorded.revision,
        })
        .await
        .expect("transition");
    let selected = sink
        .envelopes()
        .into_iter()
        .find(|envelope| envelope.kind == "transition_selected")
        .expect("selected");
    assert_eq!(
        selected.payload["metric_effects"],
        serde_json::json!(["review_return_to_work"])
    );
    assert!(
        !serde_json::to_value(TransitionCommand {
            team_session_id: started.team_session_id.clone(),
            result: "changes_requested".into(),
            deviation_reason: None,
            expected_revision: recorded.revision,
        })
        .expect("command json")
        .as_object()
        .expect("object")
        .contains_key("metric_effects"),
        "tool caller cannot inject metric_effects"
    );
}

async fn bind_sample_worker(control: &TeamControl) -> crate::TeamView {
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    control
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("node");
    let pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    control
        .bind_agent_before_start("agent-a", pending)
        .await
        .expect("bind");
    control
        .status(&started.team_session_id)
        .await
        .expect("status")
}

fn terminal_statuses(sink: &crate::RecordingSink) -> Vec<String> {
    sink.envelopes()
        .into_iter()
        .filter(|envelope| {
            envelope.kind == "agent_completed" || envelope.kind == "agent_interrupted"
        })
        .map(|envelope| {
            envelope.payload["status"]
                .as_str()
                .expect("terminal status")
                .to_string()
        })
        .collect()
}

#[tokio::test]
async fn record_agent_terminal_is_idempotent_against_active_agent_state() {
    let sink = crate::RecordingSink::default();
    let control =
        TeamControl::with_memory_store(TeamGraphCatalog::new([sample_graph()]), sink.clone());
    let started = bind_sample_worker(&control).await;
    assert_eq!(started.agents.len(), 1);

    control
        .record_agent_terminal("agent-a", "errored")
        .await
        .expect("first terminal");
    control
        .record_agent_terminal("agent-a", "completed")
        .await
        .expect("duplicate completed");
    control
        .record_agent_terminal("agent-a", "interrupted")
        .await
        .expect("duplicate interrupted");

    assert_eq!(terminal_statuses(&sink), vec!["errored".to_string()]);
    let status = control
        .status(&started.team_session_id)
        .await
        .expect("status after terminal");
    assert!(status.agents.is_empty());
}

#[tokio::test]
async fn concurrent_record_agent_terminal_appends_at_most_one_event() {
    let sink = crate::RecordingSink::default();
    let control =
        TeamControl::with_memory_store(TeamGraphCatalog::new([sample_graph()]), sink.clone());
    let started = bind_sample_worker(&control).await;

    let (first, second, third) = tokio::join!(
        control.record_agent_terminal("agent-a", "completed"),
        control.record_agent_terminal("agent-a", "errored"),
        control.record_agent_terminal("agent-a", "interrupted"),
    );
    first.expect("first");
    second.expect("second");
    third.expect("third");

    assert_eq!(terminal_statuses(&sink).len(), 1);
    let status = control
        .status(&started.team_session_id)
        .await
        .expect("status after concurrent terminal");
    assert!(status.agents.is_empty());
}

#[tokio::test]
async fn sqlite_reopen_restores_authority_before_recording_first_terminal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("terminal.sqlite");
    let store = crate::SqliteTeamStore::open(&path).await.expect("open");
    let control = TeamControl::with_store(
        TeamGraphCatalog::new([sample_graph()]),
        store,
        crate::RecordingSink::default(),
    );
    let started = bind_sample_worker(&control).await;
    drop(control);

    let terminal_store = crate::SqliteTeamStore::open(&path).await.expect("reopen");
    let terminal_control = TeamControl::with_store(
        TeamGraphCatalog::new([sample_graph()]),
        terminal_store,
        crate::RecordingSink::default(),
    );
    terminal_control
        .record_agent_terminal("agent-a", "interrupted")
        .await
        .expect("first operation records terminal");
    drop(terminal_control);

    let restored_store = crate::SqliteTeamStore::open(&path).await.expect("reopen");
    let restored = TeamControl::with_store(
        TeamGraphCatalog::new([sample_graph()]),
        restored_store,
        crate::RecordingSink::default(),
    );
    restored.restore().await.expect("restore");
    let status = restored
        .status(&started.team_session_id)
        .await
        .expect("restored status");
    assert!(status.agents.is_empty());
    let events = crate::TeamStore::load_events(
        &crate::SqliteTeamStore::open(&path)
            .await
            .expect("events store"),
        &started.team_session_id,
    )
    .await
    .expect("load events");
    let terminals: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.payload, crate::TeamEventPayload::AgentTerminal { .. }))
        .collect();
    assert_eq!(terminals.len(), 1);
}
