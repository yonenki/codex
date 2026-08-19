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
    let ended = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: started.revision,
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
        .pending_binding_for_node(&started.team_session_id, "textil_worker_default")
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
    control
        .bind_agent_before_start("acp-2", pending)
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
