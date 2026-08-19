use crate::ids::EventId;
use crate::ids::NodeRunId;
use crate::ids::TeamSessionId;
use chrono::DateTime;
use chrono::Utc;
use codex_team_graph::GraphHash;
use codex_team_graph::NodeId;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamEventKind {
    TeamStarted,
    TeamCompleted,
    TeamAborted,
    NodeStarted,
    NodeCompleted,
    AgentAttached,
    AgentCompleted,
    AgentInterrupted,
    ToolOperationStarted,
    ToolOperationCompleted,
    ToolOperationFailed,
    ToolCoverageUnreported,
    EvidenceRecorded,
    EvidenceInvalidated,
    EvidenceReused,
    TransitionRecommended,
    TransitionSelected,
    DeviationRecorded,
    AgentWaitEntered,
    AgentWaitResolved,
    ExternalWaitEntered,
    ExternalWaitResolved,
}

impl TeamEventKind {
    pub fn bumps_revision(self) -> bool {
        !matches!(
            self,
            Self::ToolOperationStarted
                | Self::ToolOperationCompleted
                | Self::ToolOperationFailed
                | Self::ToolCoverageUnreported
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TeamEventPayload {
    TeamStarted {
        task_ref: Option<String>,
        worktree: Option<String>,
        branch: Option<String>,
    },
    TeamClosed {
        reason: String,
    },
    NodeStarted {
        purpose: String,
    },
    /// Node 完了。candidate SHA と evidence は明示欄だけを正本とし、欠ける場合は null にする。
    NodeCompleted {
        result: String,
        #[serde(default)]
        candidate_sha: Option<String>,
        #[serde(default)]
        evidence_id: Option<String>,
        /// この結果を QA 集計へ載せるときに true にする。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        qa: Option<bool>,
        /// Review 指摘数。無いときは Review 結果として数えない。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        findings: Option<u32>,
    },
    AgentAttached {
        role: String,
        /// 明示的な backend fallback のときだけ true にする。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        backend_fallback: Option<bool>,
    },
    AgentTerminal {
        status: String,
    },
    ToolOperation {
        tool_name: String,
        call_id: String,
        coverage: Option<String>,
    },
    Evidence {
        evidence_id: String,
        identity: Option<String>,
    },
    Transition {
        result: Option<String>,
        to: Option<String>,
        recommended: bool,
        deviation_reason: Option<String>,
    },
    AgentWait {
        target: String,
        reason: String,
    },
    ExternalWait {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamEvent {
    pub event_id: EventId,
    pub team_session_id: TeamSessionId,
    pub sequence: u64,
    pub kind: TeamEventKind,
    pub occurred_at: DateTime<Utc>,
    pub graph_name: String,
    pub graph_version: String,
    pub graph_hash: GraphHash,
    pub node_id: Option<NodeId>,
    pub node_run_id: Option<NodeRunId>,
    pub attempt: Option<u32>,
    pub agent_thread_id: Option<String>,
    pub role: Option<String>,
    pub payload: TeamEventPayload,
}

impl TeamEvent {
    pub fn new(
        team_session_id: TeamSessionId,
        sequence: u64,
        kind: TeamEventKind,
        graph_name: String,
        graph_version: String,
        graph_hash: GraphHash,
        payload: TeamEventPayload,
    ) -> Self {
        Self {
            event_id: EventId::generate(),
            team_session_id,
            sequence,
            kind,
            occurred_at: Utc::now(),
            graph_name,
            graph_version,
            graph_hash,
            node_id: None,
            node_run_id: None,
            attempt: None,
            agent_thread_id: None,
            role: None,
            payload,
        }
    }
}
