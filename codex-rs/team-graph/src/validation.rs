use crate::error::TeamGraphError;
use crate::error::TeamGraphResult;
use crate::graph::TeamGraph;
use crate::ids::MAX_COMPLETION_CHARS;
use crate::ids::MAX_GUIDE_CHARS;
use crate::ids::MAX_NODE_PROMPT_CHARS;
use crate::ids::MAX_PURPOSE_CHARS;
use crate::ids::NodeId;
use crate::ids::SUPPORTED_SCHEMA_VERSION;
use crate::ids::validate_bounded_text;
use crate::ids::validate_id;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::collections::VecDeque;

pub fn validate_team_graph(
    graph: &TeamGraph,
    known_roles: &BTreeSet<String>,
) -> TeamGraphResult<()> {
    if graph.schema_version != SUPPORTED_SCHEMA_VERSION {
        return Err(TeamGraphError::invalid(format!(
            "unsupported schema_version {}; expected {SUPPORTED_SCHEMA_VERSION}",
            graph.schema_version
        )));
    }
    validate_id("graph name", &graph.name).map_err(TeamGraphError::invalid)?;
    validate_id("graph version", &graph.version).map_err(TeamGraphError::invalid)?;
    validate_bounded_text("description", &graph.description, MAX_PURPOSE_CHARS)
        .map_err(TeamGraphError::invalid)?;
    if graph.nodes.is_empty() {
        return Err(TeamGraphError::invalid(
            "graph must declare at least one node",
        ));
    }
    if !graph.nodes.contains_key(&graph.start) {
        return Err(TeamGraphError::invalid(format!(
            "start node '{}' does not exist",
            graph.start
        )));
    }
    if graph.terminals.is_empty() {
        return Err(TeamGraphError::invalid(
            "graph must declare at least one terminal node",
        ));
    }
    let mut seen_terminals = HashSet::new();
    for terminal in &graph.terminals {
        if !seen_terminals.insert(terminal) {
            return Err(TeamGraphError::invalid(format!(
                "duplicate terminal '{}'",
                terminal.as_str()
            )));
        }
        if !graph.nodes.contains_key(terminal) {
            return Err(TeamGraphError::invalid(format!(
                "terminal node '{}' does not exist",
                terminal.as_str()
            )));
        }
    }

    for node in graph.nodes.values() {
        validate_bounded_text("node purpose", &node.purpose, MAX_PURPOSE_CHARS)
            .map_err(TeamGraphError::invalid)?;
        validate_bounded_text("node prompt", &node.prompt, MAX_NODE_PROMPT_CHARS)
            .map_err(TeamGraphError::invalid)?;
        validate_bounded_text("node completion", &node.completion, MAX_COMPLETION_CHARS)
            .map_err(TeamGraphError::invalid)?;
        if let Some(role) = &node.role
            && !known_roles.contains(role.as_str())
        {
            return Err(TeamGraphError::invalid(format!(
                "unknown role '{}' on node '{}'",
                role.as_str(),
                node.id
            )));
        }
        for recommended in &node.recommended_tools {
            if !node.available_tools.contains(recommended) {
                return Err(TeamGraphError::invalid(format!(
                    "recommended tool '{}' on node '{}' is not available",
                    recommended.as_str(),
                    node.id
                )));
            }
        }
        let mut seen_results = HashSet::new();
        for transition in &node.transitions {
            validate_id("transition result", &transition.on).map_err(TeamGraphError::invalid)?;
            if !seen_results.insert(&transition.on) {
                return Err(TeamGraphError::invalid(format!(
                    "duplicate transition result '{}' on node '{}'",
                    transition.on, node.id
                )));
            }
            if !graph.nodes.contains_key(&transition.to) {
                return Err(TeamGraphError::invalid(format!(
                    "transition '{}' on node '{}' targets unknown node '{}'",
                    transition.on, node.id, transition.to
                )));
            }
            if !transition.guide.is_empty() {
                validate_bounded_text("transition guide", &transition.guide, MAX_GUIDE_CHARS)
                    .map_err(TeamGraphError::invalid)?;
            }
        }
        if graph.is_terminal(&node.id) && !node.transitions.is_empty() {
            return Err(TeamGraphError::invalid(format!(
                "terminal node '{}' cannot declare transitions",
                node.id
            )));
        }
        if !graph.is_terminal(&node.id) && node.transitions.is_empty() {
            return Err(TeamGraphError::invalid(format!(
                "non-terminal node '{}' must declare at least one transition",
                node.id
            )));
        }
    }

    let reachable = reachable_nodes(graph);
    for id in graph.nodes.keys() {
        if !reachable.contains(id) {
            return Err(TeamGraphError::invalid(format!(
                "node '{id}' is not reachable from start"
            )));
        }
    }
    if !graph
        .terminals
        .iter()
        .any(|terminal| reachable.contains(terminal))
    {
        return Err(TeamGraphError::invalid(
            "no terminal node is reachable from start",
        ));
    }
    Ok(())
}

fn reachable_nodes(graph: &TeamGraph) -> HashSet<NodeId> {
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back(graph.start.clone());
    while let Some(id) = queue.pop_front() {
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(node) = graph.nodes.get(&id) {
            for transition in &node.transitions {
                queue.push_back(transition.to.clone());
            }
        }
    }
    seen
}
