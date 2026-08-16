use super::with_role_developer_instructions;

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
