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
        },
        QueuedMessage {
            content: "second".to_string(),
            trigger_turn: true,
        },
    ]);

    assert_eq!(
        prepend_queued_messages(&mut queue, "third".to_string()),
        "first\n\nsecond\n\nthird"
    );
    assert!(queue.is_empty());
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
