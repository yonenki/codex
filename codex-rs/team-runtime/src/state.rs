use crate::binding::TeamAgentBinding;
use crate::ids::NodeRunId;
use crate::ids::StateRevision;
use crate::ids::TeamSessionId;
use chrono::DateTime;
use chrono::Utc;
use codex_team_graph::GraphHash;
use codex_team_graph::NodeId;
use codex_team_graph::TeamGraph;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamLifecycle {
    Running,
    WaitingAgent,
    WaitingExternal,
    NeedsAttention,
    Completed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRun {
    pub node_run_id: NodeRunId,
    pub node_id: NodeId,
    pub attempt: u32,
    pub started_at: DateTime<Utc>,
    pub result: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TeamSessionState {
    pub team_session_id: TeamSessionId,
    pub graph: TeamGraph,
    pub graph_hash: GraphHash,
    pub revision: StateRevision,
    pub next_sequence: u64,
    pub lifecycle: TeamLifecycle,
    pub current_node_id: NodeId,
    pub current_node_run: Option<NodeRun>,
    pub last_result: Option<String>,
    pub task_ref: Option<String>,
    pub worktree: Option<String>,
    pub branch: Option<String>,
    pub candidate_sha: Option<String>,
    pub agents: BTreeMap<String, TeamAgentBinding>,
    pub evidence: BTreeMap<String, String>,
    pub waiting_reason: Option<String>,
}

impl TeamSessionState {
    pub fn start(
        team_session_id: TeamSessionId,
        graph: TeamGraph,
        task_ref: Option<String>,
        worktree: Option<String>,
        branch: Option<String>,
    ) -> Self {
        let graph_hash = codex_team_graph::hash_graph(&graph);
        let start = graph.start.clone();
        Self {
            team_session_id,
            graph,
            graph_hash,
            revision: StateRevision::new(1),
            next_sequence: 2,
            lifecycle: TeamLifecycle::Running,
            current_node_id: start,
            current_node_run: None,
            last_result: None,
            task_ref,
            worktree,
            branch,
            candidate_sha: None,
            agents: BTreeMap::new(),
            evidence: BTreeMap::new(),
            waiting_reason: None,
        }
    }

    pub fn is_open(&self) -> bool {
        !matches!(
            self.lifecycle,
            TeamLifecycle::Completed | TeamLifecycle::Aborted
        )
    }

    pub fn node_run(&self, node_run_id: &NodeRunId) -> Option<&NodeRun> {
        self.current_node_run
            .as_ref()
            .filter(|run| run.node_run_id == *node_run_id)
    }
}
