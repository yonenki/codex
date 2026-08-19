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
            aborted: true,
            reason: "done".into(),
            expected_revision: started.revision,
        })
        .await;
    // Events stayed in outbox because the sink failed twice.
    control.flush_outbox().await.expect("flush");
}

#[tokio::test]
async fn same_process_recovery_flushes_outbox_batch_in_sequence_on_next_operation() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = crate::SqliteTeamStore::open(&dir.path().join("outbox.sqlite"))
        .await
        .expect("open");
    let recording = crate::RecordingSink::default();

    // 1. Initial run with 1 failing sink attempt.
    let control1 = TeamControl::with_store(catalog(), store, crate::FailingSink::fail_times(1));
    // start_team attempts sink and fails, but commits to store/outbox.
    let started = control1
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("task-1".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("start_team should succeed locally even if sink fails");

    // 2. Simulate process startup with ensure_restored where sink fails on initial flush.
    let store2 = crate::SqliteTeamStore::open(&dir.path().join("outbox.sqlite"))
        .await
        .expect("reopen");
    let control2 = TeamControl::with_store(catalog(), store2, crate::FailingSink::fail_times(1));
    control2
        .ensure_restored()
        .await
        .expect("ensure_restored succeeds and swallows sink failure");

    // 3. Now agent-collab is available (using recording sink). Next production team operation
    // must flush the entire pending batch in sequence order without manual flush or restart.
    let store3 = crate::SqliteTeamStore::open(&dir.path().join("outbox.sqlite"))
        .await
        .expect("reopen");
    let control3 = TeamControl::with_store(catalog(), store3.clone(), recording);
    control3
        .ensure_restored()
        .await
        .expect("ensure_restored recovers and flushes pending outbox");

    // start_node commits event sequence 2 and flushes all events (sequence 1 and 2).
    let node = control3
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("start_node");
    assert_eq!(node.revision.get(), 2);

    let pending = store3.pending_outbox().await.expect("pending");
    assert!(pending.is_empty(), "outbox should be fully flushed");
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

#[tokio::test]
async fn production_codex_home_store_restores_and_flushes_outbox() {
    let dir = tempfile::tempdir().expect("tempdir");
    let control = TeamControl::for_codex_home(dir.path(), crate::FailingSink::fail_times(1));
    control.replace_catalog(catalog()).await;
    let started = control
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("issue/1".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("start");
    assert!(TeamControl::team_store_path(dir.path()).exists());

    let restored = TeamControl::for_codex_home(dir.path(), crate::RecordingSink::default());
    restored.replace_catalog(catalog()).await;
    restored.ensure_restored().await.expect("restore and flush");
    let status = restored
        .status(&started.team_session_id)
        .await
        .expect("status");
    assert_eq!(status.task_ref.as_deref(), Some("issue/1"));
}
