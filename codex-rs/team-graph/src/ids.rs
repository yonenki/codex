use serde::Deserialize;
use serde::Serialize;
use std::fmt;
use std::str::FromStr;

pub const SUPPORTED_SCHEMA_VERSION: u32 = 1;
pub const MAX_NODE_PROMPT_CHARS: usize = 4000;
pub const MAX_GUIDE_CHARS: usize = 800;
pub const MAX_PURPOSE_CHARS: usize = 800;
pub const MAX_COMPLETION_CHARS: usize = 800;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_id("node id", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for NodeId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RoleName(String);

impl RoleName {
    pub fn new(value: impl Into<String>) -> Result<Self, String> {
        let value = value.into();
        validate_id("role", &value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RoleName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCapability {
    ListTeamGraphs,
    GetTeamGraph,
    ListTeams,
    StartTeam,
    GetTeamStatus,
    StartTeamNode,
    RecordTeamResult,
    GetTeamNext,
    TransitionTeam,
    EndTeam,
    SpawnAgent,
    SendMessage,
    FollowupAgent,
    Wait,
    InterruptAgent,
    ListAgents,
}

impl ToolCapability {
    pub const ALL: [Self; 16] = [
        Self::ListTeamGraphs,
        Self::GetTeamGraph,
        Self::ListTeams,
        Self::StartTeam,
        Self::GetTeamStatus,
        Self::StartTeamNode,
        Self::RecordTeamResult,
        Self::GetTeamNext,
        Self::TransitionTeam,
        Self::EndTeam,
        Self::SpawnAgent,
        Self::SendMessage,
        Self::FollowupAgent,
        Self::Wait,
        Self::InterruptAgent,
        Self::ListAgents,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ListTeamGraphs => "list_team_graphs",
            Self::GetTeamGraph => "get_team_graph",
            Self::ListTeams => "list_teams",
            Self::StartTeam => "start_team",
            Self::GetTeamStatus => "get_team_status",
            Self::StartTeamNode => "start_team_node",
            Self::RecordTeamResult => "record_team_result",
            Self::GetTeamNext => "get_team_next",
            Self::TransitionTeam => "transition_team",
            Self::EndTeam => "end_team",
            Self::SpawnAgent => "spawn_agent",
            Self::SendMessage => "send_message",
            Self::FollowupAgent => "followup_agent",
            Self::Wait => "wait",
            Self::InterruptAgent => "interrupt_agent",
            Self::ListAgents => "list_agents",
        }
    }

    pub fn parse(value: &str) -> Result<Self, String> {
        Self::ALL
            .into_iter()
            .find(|capability| capability.as_str() == value)
            .ok_or_else(|| format!("unknown tool capability '{value}'"))
    }

    pub fn is_root_coordinator(self) -> bool {
        matches!(
            self,
            Self::ListTeamGraphs
                | Self::GetTeamGraph
                | Self::ListTeams
                | Self::StartTeam
                | Self::GetTeamStatus
                | Self::StartTeamNode
                | Self::RecordTeamResult
                | Self::GetTeamNext
                | Self::TransitionTeam
                | Self::EndTeam
                | Self::SpawnAgent
                | Self::SendMessage
                | Self::FollowupAgent
                | Self::Wait
                | Self::InterruptAgent
                | Self::ListAgents
        )
    }
}

pub(crate) fn validate_id(label: &str, value: &str) -> Result<(), String> {
    if value.is_empty() || value.len() > 64 {
        return Err(format!("{label} must be 1-64 characters"));
    }
    let valid = value
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-');
    if !valid {
        return Err(format!(
            "{label} '{value}' must use lowercase letters, digits, '_' or '-'"
        ));
    }
    Ok(())
}

pub(crate) fn validate_bounded_text(
    label: &str,
    value: &str,
    max_chars: usize,
) -> Result<(), String> {
    let chars = value.chars().count();
    if chars == 0 || chars > max_chars {
        return Err(format!("{label} must be 1-{max_chars} characters"));
    }
    Ok(())
}
