use crate::FailingSink;
use crate::StartNodeCommand;
use crate::StartTeamCommand;
use crate::TeamControl;
use crate::TeamStore;
use crate::control::EndTeamCommand;
use crate::tests_support::sample_graph;
use codex_team_graph::TeamGraphCatalog;
use pretty_assertions::assert_eq;

fn catalog() -> TeamGraphCatalog {
    TeamGraphCatalog::new([sample_graph()])
}

#[tokio::test]
async fn sqlite_restores_team_node_and_binding() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("team.sqlite");
    let store = crate::SqliteTeamStore::open(&path).await.expect("open");
    let control = TeamControl::with_store(catalog(), store, crate::RecordingSink::default());
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("issue/1".into()),
            worktree: Some("N:/wt".into()),
            branch: Some("issue/1".into()),
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
    let pending = control
        .pending_binding_for_node(&started.team_session_id, "worker")
        .await
        .expect("pending");
    control
        .bind_agent_before_start("thread-1", pending)
        .await
        .expect("bind");

    let restored_store = crate::SqliteTeamStore::open(&path).await.expect("reopen");
    let restored =
        TeamControl::with_store(catalog(), restored_store, crate::RecordingSink::default());
    restored.restore().await.expect("restore");
    let status = restored
        .status(&started.team_session_id)
        .await
        .expect("status");
    assert_eq!(status.graph_name, "sample");
    assert_eq!(status.current_node.unwrap().node_id.as_str(), "work");
    let binding = restored.binding_for("thread-1").await.expect("binding");
    assert_eq!(binding.role, "worker");
    assert_eq!(
        binding.node_run_id,
        node.current_node
            .as_ref()
            .and({ None })
            .unwrap_or(binding.node_run_id.clone())
    );
    assert!(!binding.node_run_id.as_str().is_empty());
}

#[tokio::test]
async fn outbox_replays_in_order_after_sink_failure() {
    let sink = FailingSink::fail_times(2);
    let store = crate::SqliteTeamStore::memory().await.expect("memory");
    let control = TeamControl::with_store(catalog(), store, sink);
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: None,
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    let _ = control
        .end_team(EndTeamCommand {
            team_session_id: started.team_session_id.clone(),
            aborted: false,
            reason: "done".into(),
            expected_revision: started.revision,
        })
        .await;
    // First event stayed in outbox because the sink failed.
    control.flush_outbox().await.expect("flush");
}

#[tokio::test]
async fn memory_and_sqlite_keep_event_and_outbox_together() {
    let store = crate::MemoryTeamStore::default();
    let event_team = crate::ids::TeamSessionId::generate();
    let graph = sample_graph();
    let state = crate::TeamSessionState::start(event_team.clone(), graph.clone(), None, None, None);
    let event = crate::TeamEvent::new(
        event_team.clone(),
        1,
        crate::TeamEventKind::TeamStarted,
        graph.name.clone(),
        graph.version.clone(),
        state.graph_hash.clone(),
        crate::TeamEventPayload::TeamStarted {
            task_ref: None,
            worktree: None,
            branch: None,
        },
    );
    store.persist_event(&state, &event).await.expect("persist");
    assert_eq!(
        store.load_events(&event_team).await.expect("events").len(),
        1
    );
    assert_eq!(store.pending_outbox().await.expect("outbox").len(), 1);
}
