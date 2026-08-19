use super::*;
use agent_client_protocol::schema::v1::ContentBlock;
use agent_client_protocol::schema::v1::ContentChunk;
use agent_client_protocol::schema::v1::SessionNotification;
use agent_client_protocol::schema::v1::SessionUpdate;
use agent_client_protocol::schema::v1::TextContent;

#[test]
fn combines_queued_messages_in_delivery_order() {
    let mut queue = VecDeque::from([
        QueuedMessage {
            content: "first".to_string(),
            trigger_turn: false,
            submission_id: None,
            ready_to_start: true,
            on_started: None,
        },
        QueuedMessage {
            content: "second".to_string(),
            trigger_turn: true,
            submission_id: Some("second".to_string()),
            ready_to_start: true,
            on_started: None,
        },
    ]);

    assert_eq!(
        prepend_queued_messages(&mut queue, "third".to_string()),
        "first\n\nsecond\n\nthird"
    );
    assert!(queue.is_empty());
}

#[tokio::test]
async fn queued_trigger_waits_for_release_before_starting_its_generation() {
    let manager = ExternalAgentManager::default();
    let agent_id = ThreadId::new();
    manager.register_for_tests(
        agent_id,
        ExternalAgentIdentity {
            harness: "cursor".to_string(),
            model: None,
        },
    );
    let agent = manager.agent(agent_id).expect("registered agent");
    {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.running = true;
        runtime.generation = 1;
        agent.status_tx.send_replace(AgentStatus::Running);
    }

    let submission = manager
        .submit_message(agent_id, "follow up".to_string(), true)
        .expect("queue trigger");
    assert!(submission.requests_turn());

    finish_turn(
        Arc::clone(&agent),
        1,
        AgentStatus::Completed(Some("first done".to_string())),
    );
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Completed(Some("first done".to_string())), 1))
    );

    assert!(
        submission.start().is_none(),
        "queued turn must wait for the observer to confirm Stop"
    );
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Completed(Some("first done".to_string())), 1))
    );
    manager.ack_terminal_observer(agent_id, 1);
    let pending = manager
        .take_ready_pending_start(agent_id)
        .expect("acked terminal should release the queued generation");
    pending.start().await;
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Running, 2))
    );
}

#[tokio::test]
async fn followup_after_terminal_waits_for_observer_ack() {
    let manager = ExternalAgentManager::default();
    let agent_id = ThreadId::new();
    manager.register_for_tests(
        agent_id,
        ExternalAgentIdentity {
            harness: "cursor".to_string(),
            model: None,
        },
    );
    let agent = manager.agent(agent_id).expect("registered agent");
    {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.running = true;
        runtime.generation = 1;
        agent.status_tx.send_replace(AgentStatus::Running);
    }
    finish_turn(
        Arc::clone(&agent),
        1,
        AgentStatus::Completed(Some("first done".to_string())),
    );

    let submission = manager
        .submit_message(agent_id, "follow up".to_string(), true)
        .expect("queue follow-up after terminal");
    assert!(submission.requests_turn());
    assert!(
        !submission.starts_generation_now(),
        "terminal follow-up must not start generation N+1 before Stop is confirmed"
    );
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Completed(Some("first done".to_string())), 1))
    );

    assert!(submission.start().is_none());
    manager.ack_terminal_observer(agent_id, 1);
    let pending = manager
        .take_ready_pending_start(agent_id)
        .expect("observer ack should start the waiting follow-up");
    pending.start().await;
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Running, 2))
    );
}

#[test]
fn interrupt_does_not_replace_a_terminal_waiting_for_observer_ack() {
    let manager = ExternalAgentManager::default();
    let agent_id = ThreadId::new();
    manager.register_for_tests(
        agent_id,
        ExternalAgentIdentity {
            harness: "cursor".to_string(),
            model: None,
        },
    );
    let agent = manager.agent(agent_id).expect("registered agent");
    {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.running = true;
        runtime.generation = 1;
        agent.status_tx.send_replace(AgentStatus::Running);
    }
    finish_turn(
        Arc::clone(&agent),
        1,
        AgentStatus::Completed(Some("finished first".to_string())),
    );

    manager
        .interrupt(agent_id)
        .expect("interrupt terminal agent");

    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((
            AgentStatus::Completed(Some("finished first".to_string())),
            1,
        ))
    );
    assert_eq!(
        agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .unacked_terminal_generation,
        Some(1)
    );
}

#[test]
fn crossed_terminal_publishers_keep_the_first_terminal_and_one_barrier() {
    let manager = ExternalAgentManager::default();
    let agent_id = ThreadId::new();
    manager.register_for_tests(
        agent_id,
        ExternalAgentIdentity {
            harness: "cursor".to_string(),
            model: None,
        },
    );
    let agent = manager.agent(agent_id).expect("registered agent");
    {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        runtime.running = true;
        runtime.generation = 1;
        agent.status_tx.send_replace(AgentStatus::Running);
    }

    finish_current_turn(
        Arc::clone(&agent),
        AgentStatus::Errored("transport failed".to_string()),
    );
    finish_turn(
        Arc::clone(&agent),
        1,
        AgentStatus::Errored("response dropped".to_string()),
    );

    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Errored("transport failed".to_string()), 1))
    );
    manager.ack_terminal_observer(agent_id, 1);
    let submission = manager
        .submit_message(agent_id, "retry".to_string(), true)
        .expect("later follow-up remains startable");
    assert!(submission.starts_generation_now());
}

#[test]
fn aggregates_acp_agent_message_chunks() {
    let output = Arc::new(Mutex::new(String::new()));
    for text in ["hello ", "world"] {
        append_agent_text(
            &SessionNotification::new(
                "session",
                SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::Text(
                    TextContent::new(text),
                ))),
            ),
            &output,
        );
    }

    assert_eq!(take_output(&output), "hello world");
}

#[test]
fn maps_acp_stop_reasons_without_hiding_incomplete_turns() {
    assert_eq!(
        prompt_status(StopReason::EndTurn, "done".to_string()),
        AgentStatus::Completed(Some("done".to_string()))
    );
    assert_eq!(
        prompt_status(StopReason::Cancelled, String::new()),
        AgentStatus::Interrupted
    );
    assert!(matches!(
        prompt_status(StopReason::MaxTokens, "partial".to_string()),
        AgentStatus::Errored(message)
            if message.contains("token limit") && message.contains("partial")
    ));
}

#[test]
fn bounds_acp_result_for_parent_context() {
    let response = "word ".repeat(MAX_RESULT_TOKENS * 4);
    let status = prompt_status(StopReason::EndTurn, response.clone());

    assert!(matches!(
        status,
        AgentStatus::Completed(Some(output)) if output.len() < response.len()
    ));
}

#[tokio::test]
async fn registration_publishes_immutable_backend_identity_before_harness_startup() {
    let manager = ExternalAgentManager::default();
    let agent_id = ThreadId::new();
    manager
        .register(
            agent_id,
            ResolvedExternalAgentBackend {
                harness: "cursor".to_string(),
                model: Some("cursor-grok-4.6-high".to_string()),
                command: "missing-acp-host-for-identity-test".to_string(),
                args: Vec::new(),
            },
            std::env::current_dir().expect("current dir"),
            HashMap::new(),
            None,
        )
        .expect("register external agent");

    assert_eq!(
        manager.identity(agent_id),
        Some(ExternalAgentIdentity {
            harness: "cursor".to_string(),
            model: Some("cursor-grok-4.6-high".to_string()),
        })
    );
}
