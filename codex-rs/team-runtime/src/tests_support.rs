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

pub fn review_return_toml() -> &'static str {
    r#"
schema_version = 1
name = "review-return"
version = "1"
description = "Review can return to work."
start = "review"
terminals = ["completed"]

[[nodes]]
id = "review"
purpose = "Review the candidate."
role = "reviewer"
prompt = "Approve or request changes."
completion = "A review verdict exists."
available_tools = ["record_team_result", "transition_team"]
recommended_tools = ["record_team_result", "transition_team"]

[[nodes.transitions]]
on = "changes_requested"
to = "work"
recommended = true
guide = "Return findings to Work."
metric_effects = ["review_return_to_work"]

[[nodes.transitions]]
on = "approved"
to = "completed"
recommended = true
guide = "Approve."

[[nodes]]
id = "work"
purpose = "Fix findings."
role = "worker"
prompt = "Fix the findings."
completion = "A candidate exists."
available_tools = ["record_team_result", "transition_team"]
recommended_tools = ["record_team_result"]

[[nodes.transitions]]
on = "candidate_ready"
to = "review"
recommended = true
guide = "Return to review."

[[nodes]]
id = "completed"
purpose = "Team finished."
prompt = "Stop."
completion = "Closed."
"#
}

pub fn review_return_graph() -> TeamGraph {
    let dto: TeamGraphToml = toml::from_str(review_return_toml()).expect("toml");
    let graph = TeamGraph::try_from(dto).expect("graph");
    let roles = BTreeSet::from(["reviewer".to_string(), "worker".to_string()]);
    codex_team_graph::validate_team_graph(&graph, &roles).expect("valid");
    graph
}
