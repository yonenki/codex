use crate::event::TeamEvent;
use crate::event::TeamEventKind;
use crate::event::TeamEventPayload;
use crate::ids::NodeRunId;
use crate::ids::TeamSessionId;
use crate::reducer::reduce;
use crate::reducer::replay;
use crate::state::TeamLifecycle;
use crate::state::TeamSessionState;
use crate::tests_support::sample_graph;
use pretty_assertions::assert_eq;

fn started() -> TeamSessionState {
    TeamSessionState::start(
        TeamSessionId::generate(),
        sample_graph(),
        Some("issue/1".into()),
        None,
        None,
    )
}

#[test]
fn node_start_and_transition_update_current_node() {
    let mut state = started();
    let node_run_id = NodeRunId::generate();
    let started = TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: 2,
        kind: TeamEventKind::NodeStarted,
        occurred_at: chrono::Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: Some(state.current_node_id.clone()),
        node_run_id: Some(node_run_id.clone()),
        attempt: Some(1),
        agent_thread_id: None,
        role: None,
        payload: TeamEventPayload::NodeStarted {
            purpose: "work".into(),
        },
    };
    reduce(&mut state, &started).expect("reduce start");
    assert_eq!(
        state.current_node_run.as_ref().unwrap().node_run_id,
        node_run_id
    );
    assert_eq!(state.revision.get(), 2);

    let transition = TeamEvent {
        sequence: 3,
        kind: TeamEventKind::TransitionSelected,
        payload: TeamEventPayload::Transition {
            result: Some("candidate_ready".into()),
            to: Some("completed".into()),
            recommended: true,
            deviation_reason: None,
        },
        ..started
    };
    reduce(&mut state, &transition).expect("reduce transition");
    assert_eq!(state.current_node_id.as_str(), "completed");
    assert!(state.current_node_run.is_none());
}

#[test]
fn node_completed_payload_serializes_explicit_identity_fields() {
    let present = serde_json::to_value(TeamEventPayload::NodeCompleted {
        result: "candidate_ready".into(),
        candidate_sha: Some("0123456789abcdef0123456789abcdef01234567".into()),
        evidence_id: Some("ev_work".into()),
    })
    .expect("serialize present");
    assert_eq!(present["type"], "node_completed");
    assert_eq!(present["result"], "candidate_ready");
    assert_eq!(
        present["candidate_sha"],
        "0123456789abcdef0123456789abcdef01234567"
    );
    assert_eq!(present["evidence_id"], "ev_work");

    let missing = serde_json::to_value(TeamEventPayload::NodeCompleted {
        result: "candidate_ready".into(),
        candidate_sha: None,
        evidence_id: None,
    })
    .expect("serialize missing");
    assert_eq!(missing["candidate_sha"], serde_json::Value::Null);
    assert_eq!(missing["evidence_id"], serde_json::Value::Null);

    let decoded: TeamEventPayload = serde_json::from_value(serde_json::json!({
        "type": "node_completed",
        "result": "candidate_ready"
    }))
    .expect("legacy payload without identity fields");
    assert_eq!(
        decoded,
        TeamEventPayload::NodeCompleted {
            result: "candidate_ready".into(),
            candidate_sha: None,
            evidence_id: None,
        }
    );
}

#[test]
fn agent_wait_payload_round_trips_as_its_own_event_type() {
    let payload = TeamEventPayload::AgentWait {
        target: "019c-agent".into(),
        reason: "worker result".into(),
    };
    let encoded = serde_json::to_value(&payload).expect("serialize agent wait");
    assert_eq!(
        encoded,
        serde_json::json!({
            "type": "agent_wait",
            "target": "019c-agent",
            "reason": "worker result"
        })
    );
    assert_eq!(
        serde_json::from_value::<TeamEventPayload>(encoded).expect("decode agent wait"),
        payload
    );
}

#[test]
fn agent_and_external_waits_reduce_to_distinct_lifecycles() {
    let mut state = started();
    let base = TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: 2,
        kind: TeamEventKind::AgentWaitEntered,
        occurred_at: chrono::Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: None,
        node_run_id: None,
        attempt: None,
        agent_thread_id: None,
        role: None,
        payload: TeamEventPayload::AgentWait {
            target: "agent-worker".into(),
            reason: "worker result".into(),
        },
    };
    reduce(&mut state, &base).expect("enter agent wait");
    assert_eq!(state.lifecycle, TeamLifecycle::WaitingAgent);
    assert_eq!(state.waiting_reason, None);

    reduce(
        &mut state,
        &TeamEvent {
            sequence: 3,
            kind: TeamEventKind::AgentWaitResolved,
            ..base.clone()
        },
    )
    .expect("resolve agent wait");
    assert_eq!(state.lifecycle, TeamLifecycle::Running);

    reduce(
        &mut state,
        &TeamEvent {
            sequence: 4,
            kind: TeamEventKind::ExternalWaitEntered,
            payload: TeamEventPayload::ExternalWait {
                reason: "approval".into(),
            },
            ..base
        },
    )
    .expect("enter external wait");
    assert_eq!(state.lifecycle, TeamLifecycle::WaitingExternal);
    assert_eq!(state.waiting_reason.as_deref(), Some("approval"));
}

#[test]
fn node_completed_applies_explicit_candidate_sha_and_evidence() {
    let mut state = started();
    let node_run_id = NodeRunId::generate();
    let started_event = TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: 2,
        kind: TeamEventKind::NodeStarted,
        occurred_at: chrono::Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: Some(state.current_node_id.clone()),
        node_run_id: Some(node_run_id),
        attempt: Some(1),
        agent_thread_id: None,
        role: None,
        payload: TeamEventPayload::NodeStarted {
            purpose: "work".into(),
        },
    };
    reduce(&mut state, &started_event).expect("reduce start");

    let completed = TeamEvent {
        sequence: 3,
        kind: TeamEventKind::NodeCompleted,
        payload: TeamEventPayload::NodeCompleted {
            result: "candidate_ready".into(),
            candidate_sha: Some("0123456789abcdef0123456789abcdef01234567".into()),
            evidence_id: Some("ev_work".into()),
        },
        ..started_event.clone()
    };
    reduce(&mut state, &completed).expect("reduce completed");
    assert_eq!(
        state.candidate_sha.as_deref(),
        Some("0123456789abcdef0123456789abcdef01234567")
    );
    assert_eq!(
        state.evidence.get("ev_work").map(String::as_str),
        Some("0123456789abcdef0123456789abcdef01234567")
    );
    assert_eq!(state.last_result.as_deref(), Some("candidate_ready"));

    let replayed = replay(
        TeamSessionState::start(
            started_event.team_session_id.clone(),
            sample_graph(),
            Some("issue/1".into()),
            None,
            None,
        ),
        &[started_event, completed],
    )
    .expect("replay");
    assert_eq!(replayed.candidate_sha, state.candidate_sha);
    assert_eq!(replayed.evidence, state.evidence);
    assert_eq!(replayed.last_result, state.last_result);
}

#[test]
fn evidence_recorded_does_not_infer_candidate_sha() {
    let mut state = started();
    let event = TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: 2,
        kind: TeamEventKind::EvidenceRecorded,
        occurred_at: chrono::Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: None,
        node_run_id: None,
        attempt: None,
        agent_thread_id: None,
        role: None,
        payload: TeamEventPayload::Evidence {
            evidence_id: "looks_like_sha".into(),
            identity: Some("abcdef0".into()),
        },
    };
    reduce(&mut state, &event).expect("reduce evidence");
    assert_eq!(state.candidate_sha, None);
    assert_eq!(
        state.evidence.get("looks_like_sha").map(String::as_str),
        Some("abcdef0")
    );
}

#[test]
fn tool_telemetry_does_not_bump_revision() {
    let mut state = started();
    let revision = state.revision;
    let event = TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: 2,
        kind: TeamEventKind::ToolOperationStarted,
        occurred_at: chrono::Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: None,
        node_run_id: None,
        attempt: None,
        agent_thread_id: None,
        role: None,
        payload: TeamEventPayload::ToolOperation {
            tool_name: "shell".into(),
            call_id: "c1".into(),
            coverage: None,
        },
    };
    reduce(&mut state, &event).expect("reduce");
    assert_eq!(state.revision, revision);
    assert_eq!(state.lifecycle, TeamLifecycle::Running);
}

#[test]
fn rejects_cross_team_event() {
    let mut state = started();
    let mut event = TeamEvent::new(
        TeamSessionId::generate(),
        2,
        TeamEventKind::TeamCompleted,
        state.graph.name.clone(),
        state.graph.version.clone(),
        state.graph_hash.clone(),
        TeamEventPayload::TeamClosed {
            reason: "done".into(),
        },
    );
    event.sequence = 2;
    let err = reduce(&mut state, &event).expect_err("cross team");
    assert!(err.to_string().contains("cannot reference"));
}
