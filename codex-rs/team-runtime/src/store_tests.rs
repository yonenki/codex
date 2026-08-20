use crate::FailingSink;
use crate::StartNodeCommand;
use crate::StartTeamCommand;
use crate::TeamControl;
use crate::TeamStore;
use crate::control::EndTeamCommand;
use crate::tests_support::sample_graph;
use codex_team_graph::TeamGraphCatalog;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Clone)]
struct RecoveringRecordingSink {
    remaining_failures: Arc<Mutex<u32>>,
    envelopes: Arc<Mutex<Vec<crate::TeamEventEnvelope>>>,
}

impl RecoveringRecordingSink {
    fn fail_once() -> Self {
        Self {
            remaining_failures: Arc::new(Mutex::new(1)),
            envelopes: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn envelopes(&self) -> Vec<crate::TeamEventEnvelope> {
        self.envelopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crate::TeamEventSink for RecoveringRecordingSink {
    async fn publish(&self, events: &[crate::TeamEvent]) -> crate::TeamRuntimeResult<()> {
        let mut remaining = self
            .remaining_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *remaining > 0 {
            *remaining -= 1;
            return Err(crate::TeamRuntimeError::Sink("startup unavailable".into()));
        }
        drop(remaining);
        self.envelopes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(events.iter().map(crate::TeamEventEnvelope::from_event));
        Ok(())
    }
}

fn catalog() -> TeamGraphCatalog {
    TeamGraphCatalog::new([sample_graph()])
}

#[tokio::test]
async fn checked_binding_lookup_preserves_store_failure() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join("team-sessions.sqlite"))
        .expect("make the database path unusable as a file");
    let control = TeamControl::for_codex_home(dir.path(), crate::RecordingSink::default());

    let error = control
        .binding_for_checked("caller")
        .await
        .expect_err("restore failure must not become an unbound caller");
    assert!(matches!(error, crate::TeamRuntimeError::Store(_)));
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
    // Seed a pending event for the process-start recovery path.
    let control1 = TeamControl::with_store(catalog(), store, crate::FailingSink::fail_times(1));
    let started = control1
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("task-1".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("start_team should succeed locally even if sink fails");

    // One process/control/sink starts unavailable, then becomes healthy after that attempt.
    let store2 = crate::SqliteTeamStore::open(&dir.path().join("outbox.sqlite"))
        .await
        .expect("reopen");
    let sink = RecoveringRecordingSink::fail_once();
    let control2 = TeamControl::with_store(catalog(), store2.clone(), sink.clone());
    control2
        .ensure_restored()
        .await
        .expect("ensure_restored succeeds and swallows sink failure");
    assert_eq!(
        store2
            .pending_outbox()
            .await
            .expect("pending after startup failure")
            .len(),
        1
    );

    // The next production mutation commits sequence 2, then the now-healthy same sink publishes
    // the existing pending event and new event together without a restart or manual flush.
    let node = control2
        .start_node(StartNodeCommand {
            team_session_id: started.team_session_id.clone(),
            node_id: None,
            expected_revision: started.revision,
        })
        .await
        .expect("start_node");
    assert_eq!(node.revision.get(), 2);

    let envelopes = sink.envelopes();
    assert_eq!(
        envelopes
            .iter()
            .map(|event| (event.team_session_id.clone(), event.sequence))
            .collect::<Vec<_>>(),
        vec![
            (started.team_session_id.clone(), 1),
            (started.team_session_id.clone(), 2),
        ]
    );
    assert!(
        store2
            .pending_outbox()
            .await
            .expect("pending after recovery")
            .is_empty(),
        "outbox should be fully flushed"
    );
}

#[derive(Clone)]
struct PartialFailRecordingSink {
    fail_on_publish: Arc<Mutex<Option<usize>>>,
    batches: Arc<Mutex<Vec<Vec<crate::TeamEventEnvelope>>>>,
}

impl PartialFailRecordingSink {
    fn fail_on_second_publish() -> Self {
        Self {
            fail_on_publish: Arc::new(Mutex::new(Some(2))),
            batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn batches(&self) -> Vec<Vec<crate::TeamEventEnvelope>> {
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl crate::TeamEventSink for PartialFailRecordingSink {
    async fn publish(&self, events: &[crate::TeamEvent]) -> crate::TeamRuntimeResult<()> {
        let mut fail_on = self
            .fail_on_publish
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_index = self
            .batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
            + 1;
        if *fail_on == Some(next_index) {
            *fail_on = None;
            return Err(crate::TeamRuntimeError::Sink("chunk failed".into()));
        }
        drop(fail_on);
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(
                events
                    .iter()
                    .map(crate::TeamEventEnvelope::from_event)
                    .collect(),
            );
        Ok(())
    }
}

#[tokio::test]
async fn flush_outbox_chunks_pending_across_teams_and_stops_on_failed_chunk() {
    let store = crate::SqliteTeamStore::memory().await.expect("memory");
    let seed = TeamControl::with_store(catalog(), store.clone(), FailingSink::fail_times(1_000));
    let team_a = seed
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("team-a".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("start a");
    let team_b = seed
        .start_team(StartTeamCommand {
            graph_name: "sample".into(),
            task_ref: Some("team-b".into()),
            worktree: None,
            branch: None,
        })
        .await
        .expect("start b");
    let node_a = seed
        .start_node(StartNodeCommand {
            team_session_id: team_a.team_session_id.clone(),
            node_id: None,
            expected_revision: team_a.revision,
        })
        .await
        .expect("node a");
    let _ = node_a;
    let node_b = seed
        .start_node(StartNodeCommand {
            team_session_id: team_b.team_session_id.clone(),
            node_id: None,
            expected_revision: team_b.revision,
        })
        .await
        .expect("node b");
    let _ = node_b;
    for index in 0..52 {
        seed.record_deviation(&team_a.team_session_id, &format!("a-{index}"))
            .await
            .expect("dev a");
        seed.record_deviation(&team_b.team_session_id, &format!("b-{index}"))
            .await
            .expect("dev b");
    }

    let pending_before = store.pending_outbox().await.expect("pending before flush");
    assert!(
        pending_before.len() > crate::TEAM_EVENTS_MAX_BATCH,
        "need more than one ingest batch, got {}",
        pending_before.len()
    );
    let team_ids: std::collections::BTreeSet<_> = pending_before
        .iter()
        .map(|event| event.team_session_id.clone())
        .collect();
    assert_eq!(team_ids.len(), 2, "pending must include both teams");

    let sink = PartialFailRecordingSink::fail_on_second_publish();
    let flusher = TeamControl::with_store(catalog(), store.clone(), sink.clone());
    let first = flusher.flush_outbox().await;
    assert!(first.is_err(), "second chunk should stop the flush");
    let first_batches = sink.batches();
    assert_eq!(first_batches.len(), 1);
    assert_eq!(first_batches[0].len(), crate::TEAM_EVENTS_MAX_BATCH);
    assert_team_sequences_ordered(&first_batches[0]);

    let pending_after_partial = store.pending_outbox().await.expect("pending after partial");
    assert_eq!(
        pending_after_partial.len(),
        pending_before.len() - crate::TEAM_EVENTS_MAX_BATCH
    );
    assert_eq!(
        pending_after_partial[0].event_id,
        pending_before[crate::TEAM_EVENTS_MAX_BATCH].event_id,
        "retry resumes from the remaining pending head"
    );

    flusher.flush_outbox().await.expect("retry remaining");
    let all_batches = sink.batches();
    assert_eq!(all_batches.len(), 2);
    assert!(all_batches[1].len() <= crate::TEAM_EVENTS_MAX_BATCH);
    assert!(all_batches[1].len() > 0);
    assert_team_sequences_ordered(&all_batches[1]);
    assert!(
        store
            .pending_outbox()
            .await
            .expect("pending after retry")
            .is_empty()
    );

    let published: Vec<_> = all_batches.into_iter().flatten().collect();
    assert_eq!(published.len(), pending_before.len());
    assert_per_team_sequences_complete(&published);
}

fn assert_team_sequences_ordered(batch: &[crate::TeamEventEnvelope]) {
    let mut last: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for envelope in batch {
        let team = envelope.team_session_id.to_string();
        if let Some(previous) = last.insert(team.clone(), envelope.sequence) {
            assert!(
                envelope.sequence > previous,
                "team {team} sequence must stay ordered inside a chunk"
            );
        }
    }
}

fn assert_per_team_sequences_complete(published: &[crate::TeamEventEnvelope]) {
    let mut by_team: std::collections::BTreeMap<String, Vec<u64>> =
        std::collections::BTreeMap::new();
    for envelope in published {
        by_team
            .entry(envelope.team_session_id.to_string())
            .or_default()
            .push(envelope.sequence);
    }
    for (team, mut sequences) in by_team {
        sequences.sort_unstable();
        let expected: Vec<u64> = (1..=sequences.len() as u64).collect();
        assert_eq!(
            sequences, expected,
            "team {team} must reconverge without a sequence gap"
        );
    }
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
