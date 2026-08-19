use crate::discover_team_graphs;
use crate::load_known_roles;
use pretty_assertions::assert_eq;
use std::fs;
use tempfile::tempdir;

#[test]
fn discovers_and_validates_toml_graphs() {
    let dir = tempdir().expect("tempdir");
    let teams = dir.path().join(".codex").join("teams");
    let agents = dir.path().join(".codex").join("agents");
    fs::create_dir_all(&teams).expect("teams");
    fs::create_dir_all(&agents).expect("agents");
    fs::write(
        agents.join("worker.toml"),
        "name = \"textil_worker_default\"\n",
    )
    .expect("role");
    fs::write(
        teams.join("sample.toml"),
        r#"
schema_version = 1
name = "sample"
version = "1"
description = "Discovered graph."
start = "work"
terminals = ["completed"]

[[nodes]]
id = "work"
purpose = "Work."
role = "textil_worker_default"
prompt = "Do the work."
completion = "Done."
available_tools = ["spawn_agent", "transition_team"]
recommended_tools = ["spawn_agent"]

[[nodes.transitions]]
on = "done"
to = "completed"
recommended = true
guide = "Finish."

[[nodes]]
id = "completed"
purpose = "Done."
prompt = "Stop."
completion = "Closed."
"#,
    )
    .expect("graph");

    let roles = load_known_roles(dir.path(), []);
    assert!(roles.contains("textil_worker_default"));
    let catalog = discover_team_graphs(dir.path(), None, &roles).expect("catalog");
    let summaries = catalog.summaries();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0].name, "sample");
    assert!(catalog.get("sample").is_some());
}

#[test]
fn skips_missing_teams_directories() {
    let dir = tempdir().expect("tempdir");
    let catalog = discover_team_graphs(dir.path(), None, &Default::default()).expect("empty");
    assert!(catalog.summaries().is_empty());
}
