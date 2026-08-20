use crate::ids::NodeRunId;
use crate::ids::TeamSessionId;
use codex_team_graph::NodeId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentBackend {
    Native,
    Acp,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentBackendIdentity {
    Native {
        model: String,
    },
    Acp {
        harness: String,
        model: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingAgentAttachMetadata {
    pub delegation_message: String,
    pub identity: Option<AgentBackendIdentity>,
}

impl PendingAgentAttachMetadata {
    pub fn new(delegation_message: String) -> Self {
        Self {
            delegation_message,
            identity: None,
        }
    }
}

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
    pub backend_fallback: bool,
    #[serde(skip)]
    pub attach_metadata: Option<PendingAgentAttachMetadata>,
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
            backend_fallback: false,
            attach_metadata: None,
        }
    }
}
