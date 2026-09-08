use crate::tests_support::review_return_graph;
use crate::*;
use codex_team_graph::TeamGraphCatalog;
use pretty_assertions::assert_eq;

fn verdict(view: &TeamView, result: &str) -> AdvanceTeamCommand {
    AdvanceTeamCommand {
        completion: RecordResultCommand {
            team_session_id: view.team_session_id.clone(),
            expected_revision: view.revision,
            result: result.into(),
            evidence_id: Some("verified-candidate".into()),
            candidate_sha: Some("candidate-sha".into()),
            qa: Some(true),
            findings: Some(1),
        },
        deviation_reason: None,
    }
}

async fn start(control: &TeamControl) -> TeamView {
    control
        .start_team(StartTeamCommand {
            graph_name: "review-return".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start")
}

#[tokio::test]
async fn advance_preserves_review_evidence_and_starts_successor_after_sqlite_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("team.sqlite");
    let catalog = TeamGraphCatalog::new([review_return_graph()]);
    let sink = RecordingSink::default();
    let control = TeamControl::with_store(
        catalog.clone(),
        SqliteTeamStore::open(&path).await.unwrap(),
        sink.clone(),
    );
    let initial = start(&control).await;
    let view = control
        .advance_team(verdict(&initial, "changes_requested"))
        .await
        .unwrap();
    assert_eq!(view.current_node.as_ref().unwrap().node_id.as_str(), "work");
    assert_eq!(view.candidate_sha.as_deref(), Some("candidate-sha"));
    assert!(
        view.recommended_next
            .iter()
            .any(|action| action.tool == codex_team_graph::ToolCapability::AdvanceTeam)
    );
    let events = sink.envelopes();
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "team_started",
            "node_started",
            "node_completed",
            "transition_selected",
            "node_started",
        ]
    );
    assert_eq!(events[2].payload["evidence_id"], "verified-candidate");
    assert_eq!(events[2].payload["qa"], true);
    assert_eq!(events[2].payload["findings"], 1);
    assert_eq!(
        events[3].payload["metric_effects"],
        serde_json::json!(["review_return_to_work"])
    );
    let restored = TeamControl::with_store(
        catalog,
        SqliteTeamStore::open(&path).await.unwrap(),
        RecordingSink::default(),
    );
    let restored_view = restored.status(&view.team_session_id).await.unwrap();
    assert_eq!(
        serde_json::to_value(restored_view).unwrap(),
        serde_json::to_value(&view).unwrap()
    );
    let next = restored
        .advance_team(verdict(&view, "candidate_ready"))
        .await
        .unwrap();
    assert_eq!(next.current_node.unwrap().node_id.as_str(), "review");
}

#[tokio::test]
async fn rejected_advance_keeps_candidate_revision_and_event_trace_unchanged() {
    let mut graph = review_return_graph();
    graph
        .nodes
        .get_mut(&graph.start)
        .unwrap()
        .transitions
        .iter_mut()
        .for_each(|transition| transition.recommended = false);
    let sink = RecordingSink::default();
    let control = TeamControl::with_memory_store(TeamGraphCatalog::new([graph]), sink.clone());
    let initial = start(&control).await;
    for result in ["undeclared", "approved"] {
        assert!(
            control
                .advance_team(verdict(&initial, result))
                .await
                .is_err()
        );
        assert_eq!(
            serde_json::to_value(control.status(&initial.team_session_id).await.unwrap()).unwrap(),
            serde_json::to_value(&initial).unwrap()
        );
        assert_eq!(sink.envelopes().len(), 1);
    }
    let mut command = verdict(&initial, "approved");
    command.deviation_reason = Some("accept reviewed candidate".into());
    let advanced = control.advance_team(command.clone()).await.unwrap();
    assert_eq!(advanced.current_node.unwrap().node_id.as_str(), "completed");
    assert!(matches!(
        control.advance_team(command).await,
        Err(TeamRuntimeError::StaleRevision { .. })
    ));
}

#[tokio::test]
async fn advance_waits_for_active_reviewer_to_finish() {
    let control = TeamControl::memory(TeamGraphCatalog::new([review_return_graph()]));
    let initial = start(&control).await;
    control
        .start_node(StartNodeCommand {
            team_session_id: initial.team_session_id.clone(),
            node_id: None,
            expected_revision: initial.revision,
        })
        .await
        .unwrap();
    let binding = control
        .pending_binding_for_node(&initial.team_session_id, "reviewer")
        .await
        .unwrap();
    control
        .bind_agent_before_start("reviewer-session", binding)
        .await
        .unwrap();
    let active = control.status(&initial.team_session_id).await.unwrap();
    assert!(matches!(
        control.advance_team(verdict(&active, "approved")).await,
        Err(TeamRuntimeError::ActiveAgents(_))
    ));
    assert_eq!(
        serde_json::to_value(control.status(&active.team_session_id).await.unwrap()).unwrap(),
        serde_json::to_value(active).unwrap()
    );
}

#[tokio::test]
async fn sqlite_batch_failure_rolls_back_events_outbox_and_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("team.sqlite");
    let control = TeamControl::with_store(
        TeamGraphCatalog::new([review_return_graph()]),
        SqliteTeamStore::open(&path).await.unwrap(),
        RecordingSink::default(),
    );
    let initial = start(&control).await;
    let store = SqliteTeamStore::open(&path).await.unwrap();
    let states = store.load_teams().await.unwrap();
    let events = store.load_events(&initial.team_session_id).await.unwrap();
    let outbox = store.pending_outbox().await.unwrap();
    let mut fresh = events[0].clone();
    fresh.event_id = EventId::generate();
    fresh.sequence = states[0].next_sequence;
    let result = store
        .persist_events(&states[0], &[fresh, events[0].clone()])
        .await;
    assert!(matches!(result, Err(TeamRuntimeError::Store(_))));
    assert_eq!(
        serde_json::to_value(store.load_teams().await.unwrap()).unwrap(),
        serde_json::to_value(states).unwrap()
    );
    assert_eq!(
        serde_json::to_value(store.load_events(&initial.team_session_id).await.unwrap()).unwrap(),
        serde_json::to_value(events).unwrap()
    );
    assert_eq!(
        serde_json::to_value(store.pending_outbox().await.unwrap()).unwrap(),
        serde_json::to_value(outbox).unwrap()
    );
}
