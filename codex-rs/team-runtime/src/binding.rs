use crate::ids::NodeRunId;
use crate::ids::TeamSessionId;
use codex_team_graph::NodeId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamAgentBinding {
    pub team_session_id: TeamSessionId,
    pub node_run_id: NodeRunId,
    pub node_id: NodeId,
    pub role: String,
    pub agent_thread_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTeamBinding {
    pub team_session_id: TeamSessionId,
    pub node_run_id: NodeRunId,
    pub node_id: NodeId,
    pub role: String,
}

impl PendingTeamBinding {
    pub fn bind(self, agent_thread_id: impl Into<String>) -> TeamAgentBinding {
        TeamAgentBinding {
            team_session_id: self.team_session_id,
            node_run_id: self.node_run_id,
            node_id: self.node_id,
            role: self.role,
            agent_thread_id: agent_thread_id.into(),
        }
    }
}

impl TeamAgentBinding {
    pub fn to_pending(&self) -> PendingTeamBinding {
        PendingTeamBinding {
            team_session_id: self.team_session_id.clone(),
            node_run_id: self.node_run_id.clone(),
            node_id: self.node_id.clone(),
            role: self.role.clone(),
        }
    }
}
