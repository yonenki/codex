use crate::event::TeamEvent;
use crate::event::TeamEventKind;
use crate::event::TeamEventPayload;
use crate::ids::NodeRunId;
use crate::ids::TeamSessionId;
use crate::reducer::reduce;
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
