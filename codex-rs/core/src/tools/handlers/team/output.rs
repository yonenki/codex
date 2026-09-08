use super::*;
use codex_team_graph::NodeGuide;
use codex_team_graph::NodeId;
use codex_team_graph::RoleName;
use codex_team_graph::TeamTransition;
use codex_team_runtime::StateRevision;
use codex_team_runtime::TeamLifecycle;

pub(super) enum ViewDetail {
    Summary,
    Guide,
}

#[derive(Serialize)]
struct Progress<'a> {
    team_session_id: &'a TeamSessionId,
    revision: StateRevision,
    lifecycle: &'a TeamLifecycle,
    task_ref: &'a Option<String>,
    current_node: Option<NodeProgress<'a>>,
    last_result: &'a Option<String>,
    candidate_sha: &'a Option<String>,
    active_agents: usize,
    waiting_reason: &'a Option<String>,
    recommended_next: Vec<ToolCapability>,
}

#[derive(Serialize)]
struct NodeProgress<'a> {
    node_id: &'a NodeId,
    role: &'a Option<RoleName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    completion: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transitions: Option<&'a [TeamTransition]>,
}

impl<'a> NodeProgress<'a> {
    fn new(node: &'a NodeGuide, detail: &ViewDetail) -> Self {
        let guide = matches!(detail, ViewDetail::Guide);
        Self {
            node_id: &node.node_id,
            role: &node.role,
            prompt: guide.then_some(node.prompt.as_str()),
            completion: guide.then_some(node.completion.as_str()),
            transitions: guide.then_some(node.possible_transitions.as_slice()),
        }
    }
}

pub(super) fn progress(view: &TeamView, detail: ViewDetail) -> JsonValue {
    serde_json::json!(Progress {
        team_session_id: &view.team_session_id,
        revision: view.revision,
        lifecycle: &view.lifecycle,
        task_ref: &view.task_ref,
        current_node: view
            .current_node
            .as_ref()
            .map(|node| NodeProgress::new(node, &detail)),
        last_result: &view.last_result,
        candidate_sha: &view.candidate_sha,
        active_agents: view.agents.len(),
        waiting_reason: &view.waiting_reason,
        recommended_next: view.recommended_next.iter().map(|next| next.tool).collect(),
    })
}
