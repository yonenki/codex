use super::*;

#[test]
fn parses_antigravity_init_event() {
    let event =
        parse_stream_event(r#"{"event":"init","conversation_id":"conversation-1","init":{}}"#)
            .expect("init event should parse");

    assert!(matches!(
        event,
        StreamEvent::Conversation(conversation_id) if conversation_id == "conversation-1"
    ));
}

#[test]
fn parses_successful_antigravity_result() {
    let event =
        parse_stream_event(r#"{"event":"result","result":{"status":"SUCCESS","response":"done"}}"#)
            .expect("result event should parse");

    assert!(matches!(
        event,
        StreamEvent::Result(AgentStatus::Completed(Some(response))) if response == "done"
    ));
}

#[test]
fn preserves_antigravity_failure_response() {
    let event =
        parse_stream_event(r#"{"event":"result","result":{"status":"ERROR","response":"failed"}}"#)
            .expect("result event should parse");

    assert!(matches!(
        event,
        StreamEvent::Result(AgentStatus::Errored(response)) if response == "failed"
    ));
}

#[test]
fn bounds_successful_antigravity_result_for_parent_context() {
    let response = "word ".repeat(MAX_RESULT_TOKENS * 4);
    let event = parse_stream_event(
        &serde_json::json!({
            "event": "result",
            "result": {"status": "SUCCESS", "response": response},
        })
        .to_string(),
    )
    .expect("result event should parse");

    assert!(matches!(
        event,
        StreamEvent::Result(AgentStatus::Completed(Some(response)))
            if response.len() < "word ".repeat(MAX_RESULT_TOKENS * 4).len()
    ));
}

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
