use super::AcpBackendOverrides;
use super::FunctionCallError;
use super::explicit_backend;
use super::resolve_backend;
use super::with_role_developer_instructions;
use crate::agent::role::acp_backend;

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
        resolve_backend(AcpBackendOverrides::default(), std::slice::from_ref(&role))
            .expect("role backend"),
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
        resolve_backend(explicit, &[role]).expect("explicit override"),
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
        resolve_backend(explicit, &pool).expect("pool candidate"),
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
        resolve_backend(explicit, &pool).expect("explicit backend"),
        acp_backend("kimi".to_string(), None, None)
    );
}

#[test]
fn backend_is_required_without_an_acp_backed_role() {
    let error = resolve_backend(AcpBackendOverrides::default(), &[])
        .expect_err("missing backend must fail");
    assert!(
        matches!(error, FunctionCallError::RespondToModel(message) if message.contains("harness is required"))
    );
}
