use crate::dto::TeamGraphToml;
use crate::graph::TeamGraph;
use crate::validate_team_graph;
use pretty_assertions::assert_eq;
use std::collections::BTreeSet;

fn sample_toml() -> &'static str {
    r#"
schema_version = 1
name = "sample"
version = "1"
description = "Sample team graph."
start = "work"
terminals = ["completed"]

[[nodes]]
id = "work"
purpose = "Implement the candidate."
role = "textil_worker_default"
prompt = "Implement the approved scope and return a candidate."
completion = "A candidate SHA and worker report exist."
available_tools = ["spawn_agent", "record_team_result", "get_team_next", "transition_team"]
recommended_tools = ["spawn_agent", "record_team_result"]

[[nodes.transitions]]
on = "candidate_ready"
to = "completed"
recommended = true
guide = "Worker returned a candidate."

[[nodes]]
id = "completed"
purpose = "Team finished."
prompt = "Stop. The team is complete."
completion = "The team session is closed."
"#
}

fn parse(toml: &str) -> TeamGraph {
    let dto: TeamGraphToml = toml::from_str(toml).expect("toml");
    TeamGraph::try_from(dto).expect("graph")
}

fn roles() -> BTreeSet<String> {
    BTreeSet::from(["textil_worker_default".to_string()])
}

#[test]
fn validates_a_reachable_graph() {
    let graph = parse(sample_toml());
    validate_team_graph(&graph, &roles()).expect("valid");
    assert_eq!(graph.start.as_str(), "work");
    assert_eq!(graph.nodes.len(), 2);
}

#[test]
fn rejects_unsupported_schema_version() {
    let mut graph = parse(sample_toml());
    graph.schema_version = 2;
    let err = validate_team_graph(&graph, &roles()).expect_err("version");
    assert!(err.to_string().contains("unsupported schema_version"));
}

#[test]
fn rejects_unknown_role() {
    let graph = parse(sample_toml());
    let err = validate_team_graph(&graph, &BTreeSet::new()).expect_err("role");
    assert!(err.to_string().contains("unknown role"));
}

#[test]
fn rejects_unknown_tool_capability() {
    let err = TeamGraph::try_from(
        toml::from_str::<TeamGraphToml>(
            &sample_toml().replace("\"spawn_agent\"", "\"invent_workflow_dsl\""),
        )
        .expect("toml"),
    )
    .expect_err("capability");
    assert!(err.contains("unknown tool capability"));
}

#[test]
fn rejects_unknown_transition_target() {
    let graph = parse(&sample_toml().replace("to = \"completed\"", "to = \"missing\""));
    let err = validate_team_graph(&graph, &roles()).expect_err("target");
    assert!(err.to_string().contains("unknown node"));
}

#[test]
fn rejects_duplicate_node_ids() {
    let mut toml = sample_toml().to_string();
    toml.push_str(
        r#"

[[nodes]]
id = "work"
purpose = "Duplicate."
prompt = "Duplicate."
completion = "Duplicate."
"#,
    );
    let err = TeamGraph::try_from(toml::from_str::<TeamGraphToml>(&toml).expect("toml"))
        .expect_err("duplicate");
    assert!(err.contains("duplicate node id"));
}

#[test]
fn rejects_missing_start_and_unreachable_nodes() {
    let graph = parse(&sample_toml().replace("start = \"work\"", "start = \"completed\""));
    let err = validate_team_graph(&graph, &roles()).expect_err("unreachable");
    assert!(err.to_string().contains("not reachable"));
}

#[test]
fn rejects_oversized_prompt() {
    let long = "p".repeat(4001);
    let toml = sample_toml().replace(
        "prompt = \"Implement the approved scope and return a candidate.\"",
        &format!("prompt = \"{long}\""),
    );
    let graph = parse(&toml);
    let err = validate_team_graph(&graph, &roles()).expect_err("prompt");
    assert!(err.to_string().contains("node prompt"));
}

#[test]
fn rejects_missing_terminal() {
    let graph = parse(&sample_toml().replace("terminals = [\"completed\"]", "terminals = []"));
    let err = validate_team_graph(&graph, &roles()).expect_err("terminal");
    assert!(err.to_string().contains("at least one terminal"));
}
