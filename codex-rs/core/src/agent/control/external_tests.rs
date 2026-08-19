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

    if let Some(pending) = submission.start() {
        pending.start().await;
    }
    assert_eq!(
        manager.lifecycle_status(agent_id),
        Some((AgentStatus::Running, 2))
    );
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
