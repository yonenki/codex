use super::*;
use crate::agent::AgentStatus;
use crate::agent::role::acp_backend;
use codex_tools::ToolSpec;
use pretty_assertions::assert_eq;
use serde_json::json;

#[test]
fn role_instructions_are_prepended_without_changing_the_task() {
    assert_eq!(
        with_role_developer_instructions(
            Some("Implement only the assigned scope."),
            "Fix the issue.".to_string()
        ),
        "Role instructions:\nImplement only the assigned scope.\n\nTask:\nFix the issue."
    );
}

#[test]
fn task_is_unchanged_without_a_role() {
    assert_eq!(
        with_role_developer_instructions(None, "Fix the issue.".to_string()),
        "Fix the issue."
    );
}

#[test]
fn role_backend_is_used_without_explicit_spawn_backend() {
    let role = acp_backend(
        "grok-build".to_string(),
        Some("grok-4.6".to_string()),
        Some("high".to_string()),
    );
    assert_eq!(
        resolve_backend_candidate(
            AcpBackendOverrides::default(),
            std::slice::from_ref(&role),
            None,
        )
        .expect("role backend")
        .backend,
        role
    );
}

#[test]
fn explicit_backend_overrides_role_defaults() {
    let explicit = explicit_backend(
        Some("antigravity".to_string()),
        Some("gemini-3.7-flash".to_string()),
        Some("high".to_string()),
    )
    .expect("valid explicit backend");
    let role = acp_backend(
        "grok-build".to_string(),
        Some("grok-4.6".to_string()),
        Some("high".to_string()),
    );
    assert_eq!(
        resolve_backend_candidate(explicit, &[role], None)
            .expect("explicit override")
            .backend,
        acp_backend(
            "antigravity".to_string(),
            Some("gemini-3.7-flash".to_string()),
            Some("high".to_string()),
        )
    );
}

#[test]
fn explicit_harness_selects_matching_pool_candidate_defaults() {
    let pool = vec![
        acp_backend("grok-build".to_string(), None, None),
        acp_backend(
            "antigravity".to_string(),
            Some("gemini-3.7-flash".to_string()),
            Some("high".to_string()),
        ),
    ];
    let explicit = explicit_backend(Some("antigravity".to_string()), None, None)
        .expect("valid explicit backend");
    assert_eq!(
        resolve_backend_candidate(explicit, &pool, None)
            .expect("pool candidate")
            .backend,
        pool[1]
    );
}

#[test]
fn unrelated_explicit_harness_does_not_inherit_pool_model() {
    let pool = vec![acp_backend(
        "grok-build".to_string(),
        Some("grok-4.6".to_string()),
        Some("high".to_string()),
    )];
    let explicit =
        explicit_backend(Some("kimi".to_string()), None, None).expect("valid explicit backend");
    assert_eq!(
        resolve_backend_candidate(explicit, &pool, None)
            .expect("explicit backend")
            .backend,
        acp_backend("kimi".to_string(), None, None)
    );
}

#[test]
fn backend_is_required_without_an_acp_backed_role() {
    let error = resolve_backend_candidate(AcpBackendOverrides::default(), &[], None)
        .expect_err("missing backend must fail");
    assert!(
        matches!(error, FunctionCallError::RespondToModel(message) if message.contains("harness is required"))
    );
}

#[test]
fn fallback_selects_next_pool_candidate_without_backend_names() {
    let pool = vec![
        acp_backend(
            "grok-build".to_string(),
            Some("grok-4.6".to_string()),
            Some("high".to_string()),
        ),
        acp_backend("kimi".to_string(), Some("kimi-code/k3".to_string()), None),
    ];
    let selection = resolve_backend_candidate(AcpBackendOverrides::default(), &pool, Some(1))
        .expect("next pool candidate");
    assert_eq!(selection.backend, pool[1]);
    assert_eq!(selection.candidate_index, Some(1));
}

#[test]
fn fallback_fails_after_last_pool_candidate() {
    let pool = vec![acp_backend("grok-build".to_string(), None, None)];
    let error = resolve_backend_candidate(AcpBackendOverrides::default(), &pool, Some(1))
        .expect_err("exhausted pool must fail");
    assert!(
        matches!(error, FunctionCallError::RespondToModel(message) if message.contains("no remaining ACP backend candidate"))
    );
}

#[test]
fn fallback_requires_prior_candidate_to_reach_terminal_status() {
    for status in [
        AgentStatus::PendingInit,
        AgentStatus::Running,
        AgentStatus::Interrupted,
    ] {
        let error = ensure_fallback_source_terminal(status)
            .expect_err("non-final source must not overlap with fallback");
        assert!(
            matches!(error, FunctionCallError::RespondToModel(message) if message.contains("still active"))
        );
    }
    ensure_fallback_source_terminal(AgentStatus::Completed(Some("done".to_string())))
        .expect("completed source may fall back");
}

#[test]
fn spawn_spec_accepts_observer_metadata_string_map() {
    let ToolSpec::Function(spec) = spawn_spec() else {
        panic!("acp spawn should be a function tool");
    };
    let properties = spec
        .parameters
        .properties
        .as_ref()
        .expect("spawn parameters");
    assert!(properties.contains_key("metadata"));
}

#[test]
fn spawn_output_reports_first_fallback_explicit_and_default_model() {
    let path = AgentPath::try_from("/root/worker").expect("agent path");
    let pool = [
        acp_backend("grok-build".to_string(), Some("grok-4.6".to_string()), None),
        acp_backend(
            "cursor".to_string(),
            Some("cursor-grok-4.6-high".to_string()),
            None,
        ),
    ];
    let cases = [(None, &pool[0]), (Some(1), &pool[1])];
    for (fallback_index, expected) in cases {
        let selected =
            resolve_backend_candidate(AcpBackendOverrides::default(), &pool, fallback_index)
                .expect("pool candidate");
        assert_eq!(selected.backend, *expected);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&acp_spawn_output(
                &path,
                &selected.backend.harness,
                selected.backend.model.as_deref(),
            ))
            .expect("output json"),
            json!({
                "task_name": "/root/worker",
                "harness": expected.harness,
                "model": expected.model,
            })
        );
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&acp_spawn_output(&path, "kimi", Some("k3"),))
            .expect("output json"),
        json!({"task_name": "/root/worker", "harness": "kimi", "model": "k3"})
    );
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&acp_spawn_output(
            &path, "cursor", /*model*/ None,
        ))
        .expect("output json"),
        json!({"task_name": "/root/worker", "harness": "cursor", "model": null})
    );
}
