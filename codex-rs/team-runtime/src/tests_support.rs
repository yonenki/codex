use codex_team_graph::GraphHash;
use codex_team_graph::TeamGraph;
use codex_team_graph::TeamGraphToml;
use std::collections::BTreeSet;

pub fn sample_toml() -> &'static str {
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
role = "worker"
prompt = "Implement the approved scope."
completion = "A candidate exists."
available_tools = ["spawn_agent", "record_team_result", "get_team_next", "transition_team", "end_team"]
recommended_tools = ["spawn_agent", "record_team_result", "transition_team"]

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

pub fn sample_graph() -> TeamGraph {
    let dto: TeamGraphToml = toml::from_str(sample_toml()).expect("toml");
    let graph = TeamGraph::try_from(dto).expect("graph");
    let roles = BTreeSet::from(["worker".to_string()]);
    codex_team_graph::validate_team_graph(&graph, &roles).expect("valid");
    graph
}

pub fn sample_hash() -> GraphHash {
    codex_team_graph::hash_graph(&sample_graph())
}
