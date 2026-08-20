use crate::ids::StateRevision;
use crate::ids::TeamSessionId;
use thiserror::Error;

pub type TeamRuntimeResult<T> = Result<T, TeamRuntimeError>;

#[derive(Clone, Debug, Error)]
pub enum TeamRuntimeError {
    #[error("{0}")]
    Invalid(String),
    #[error("team session {0} was not found")]
    TeamNotFound(TeamSessionId),
    #[error("team session {0} is closed")]
    ClosedTeam(TeamSessionId),
    #[error("team session {0} has no active node run")]
    NoActiveNodeRun(TeamSessionId),
    #[error("team session {0} already has an active node run")]
    ActiveNodeRunExists(TeamSessionId),
    #[error("team session {0} has no completed node run")]
    NoCompletedNodeRun(TeamSessionId),
    #[error("team session {team} is at non-terminal node '{node}'")]
    NonTerminalNode {
        team: TeamSessionId,
        node: codex_team_graph::NodeId,
    },
    #[error("team session {0} still has active agents")]
    ActiveAgents(TeamSessionId),
    #[error("role '{actual}' does not match node role '{expected}'")]
    RoleMismatch { expected: String, actual: String },
    #[error("team session {team} cannot reference {subject} from another team")]
    CrossTeamRef {
        team: TeamSessionId,
        subject: String,
    },
    #[error("expected revision {expected}, actual {actual}")]
    StaleRevision {
        expected: StateRevision,
        actual: StateRevision,
    },
    #[error("team store failed: {0}")]
    Store(String),
    #[error("team event sink failed: {0}")]
    Sink(String),
}

impl TeamRuntimeError {
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::Invalid(message.into())
    }
}
