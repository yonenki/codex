//! Non-reserved `team` namespace tools.

use super::multi_agents_common::function_arguments;
use super::multi_agents_common::tool_output_code_mode_result;
use super::multi_agents_common::tool_output_json_text;
use super::multi_agents_common::tool_output_response_item;
use super::parse_arguments;
use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::context::boxed_tool_output;
use crate::tools::registry::CoreToolRuntime;
use crate::tools::registry::ToolExecutor;
use codex_protocol::models::ResponseInputItem;
use codex_team_graph::ToolCapability;
use codex_team_runtime::TeamRuntimeError;
use codex_team_runtime::TeamSessionId;
use codex_team_runtime::TeamView;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolName;
use codex_tools::ToolSpec;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

mod agents;
mod lifecycle;

pub(crate) use agents::TeamAgentToolHandler;
pub(crate) use lifecycle::TeamLifecycleToolHandler;

const TEAM_NAMESPACE: &str = "team";

pub(crate) fn team_namespace() -> &'static str {
    TEAM_NAMESPACE
}

pub(crate) fn all_team_capabilities() -> &'static [ToolCapability] {
    &ToolCapability::ALL
}

fn string_prop(description: &str) -> JsonSchema {
    JsonSchema::string(Some(description.to_string()))
}

fn object_spec(
    name: &str,
    description: &str,
    properties: BTreeMap<String, JsonSchema>,
    required: Vec<String>,
) -> ToolSpec {
    ToolSpec::Function(ResponsesApiTool {
        name: name.to_string(),
        description: description.to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(properties, Some(required), Some(false.into())),
        output_schema: None,
    })
}

#[derive(Debug)]
enum TeamToolAuthority {
    CatalogRead,
    ListTeams { bound_team: Option<TeamSessionId> },
    StartTeam,
    TeamSession(TeamSessionId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TeamAuthorityClass {
    CatalogRead,
    ListTeams,
    StartTeam,
    TeamSession,
}

fn team_authority_class(capability: ToolCapability) -> TeamAuthorityClass {
    match capability {
        ToolCapability::ListTeamGraphs | ToolCapability::GetTeamGraph => {
            TeamAuthorityClass::CatalogRead
        }
        ToolCapability::ListTeams => TeamAuthorityClass::ListTeams,
        ToolCapability::StartTeam => TeamAuthorityClass::StartTeam,
        _ => TeamAuthorityClass::TeamSession,
    }
}

#[derive(Debug, Deserialize)]
struct TeamAuthorityArgs {
    team_session_id: Option<String>,
}

async fn caller_bound_team(
    invocation: &ToolInvocation,
) -> Result<Option<TeamSessionId>, FunctionCallError> {
    let team = invocation.session.services.agent_control.team();
    Ok(team
        .binding_for_checked(&invocation.session.thread_id.to_string())
        .await
        .map_err(map_team_error)?
        .map(|binding| binding.team_session_id))
}

fn resolve_team_session_authority(
    bound_team: Option<TeamSessionId>,
    explicit_team: Option<TeamSessionId>,
) -> Result<TeamSessionId, FunctionCallError> {
    match (bound_team, explicit_team) {
        (Some(bound), Some(explicit)) if bound != explicit => {
            Err(map_team_error(TeamRuntimeError::CrossTeamRef {
                team: bound,
                subject: format!("team session {explicit}"),
            }))
        }
        (Some(bound), _) => Ok(bound),
        (None, Some(explicit)) => Ok(explicit),
        (None, None) => Err(FunctionCallError::RespondToModel(
            "root coordinator tools require team_session_id".to_string(),
        )),
    }
}

async fn authorize_team_tool(
    invocation: &ToolInvocation,
    capability: ToolCapability,
) -> Result<TeamToolAuthority, FunctionCallError> {
    match team_authority_class(capability) {
        TeamAuthorityClass::CatalogRead => Ok(TeamToolAuthority::CatalogRead),
        TeamAuthorityClass::ListTeams => Ok(TeamToolAuthority::ListTeams {
            bound_team: caller_bound_team(invocation).await?,
        }),
        TeamAuthorityClass::StartTeam => {
            if let Some(bound) = caller_bound_team(invocation).await? {
                return Err(FunctionCallError::RespondToModel(format!(
                    "Team-bound callers cannot start a new Team session; delegate start_team to an unbound root coordinator (caller is bound to team session {bound})"
                )));
            }
            Ok(TeamToolAuthority::StartTeam)
        }
        TeamAuthorityClass::TeamSession => {
            let arguments = function_arguments(invocation.payload.clone())?;
            let args: TeamAuthorityArgs = parse_arguments(&arguments)?;
            let explicit_team = args
                .team_session_id
                .map(TeamSessionId::parse)
                .transpose()
                .map_err(FunctionCallError::RespondToModel)?;
            Ok(TeamToolAuthority::TeamSession(
                resolve_team_session_authority(
                    caller_bound_team(invocation).await?,
                    explicit_team,
                )?,
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn team(id: &str) -> TeamSessionId {
        TeamSessionId::parse(id).expect("team id")
    }

    #[test]
    fn every_team_tool_has_one_central_authority_class() {
        let catalog = [ToolCapability::ListTeamGraphs, ToolCapability::GetTeamGraph];
        let list = [ToolCapability::ListTeams];
        let start = [ToolCapability::StartTeam];
        let scoped = ToolCapability::ALL
            .into_iter()
            .filter(|capability| !catalog.contains(capability))
            .filter(|capability| !list.contains(capability))
            .filter(|capability| !start.contains(capability))
            .collect::<Vec<_>>();

        assert!(
            catalog
                .into_iter()
                .all(|capability| team_authority_class(capability)
                    == TeamAuthorityClass::CatalogRead)
        );
        assert!(list
            .into_iter()
            .all(|capability| team_authority_class(capability) == TeamAuthorityClass::ListTeams));
        assert!(start
            .into_iter()
            .all(|capability| team_authority_class(capability) == TeamAuthorityClass::StartTeam));
        assert_eq!(scoped.len(), 15);
        assert!(
            scoped
                .into_iter()
                .all(|capability| team_authority_class(capability)
                    == TeamAuthorityClass::TeamSession)
        );
    }

    #[test]
    fn bound_caller_cannot_select_another_team() {
        let error = resolve_team_session_authority(Some(team("team-a")), Some(team("team-b")))
            .expect_err("cross-Team explicit id must fail");
        assert!(matches!(
            error,
            FunctionCallError::RespondToModel(message)
                if message.contains("team session team-a cannot reference team session team-b from another team")
        ));
    }

    #[test]
    fn same_team_bound_and_unbound_root_authority_are_preserved() {
        assert_eq!(
            resolve_team_session_authority(Some(team("team-a")), Some(team("team-a")))
                .expect("same Team"),
            team("team-a")
        );
        assert_eq!(
            resolve_team_session_authority(None, Some(team("team-b")))
                .expect("unbound root explicit Team"),
            team("team-b")
        );
    }
}

fn map_team_error(err: codex_team_runtime::TeamRuntimeError) -> FunctionCallError {
    FunctionCallError::RespondToModel(err.to_string())
}

#[derive(Debug, Serialize)]
struct TeamToolResult {
    #[serde(flatten)]
    value: JsonValue,
}

impl TeamToolResult {
    fn view(view: TeamView) -> Self {
        Self {
            value: serde_json::to_value(view).unwrap_or(JsonValue::Null),
        }
    }

    fn json(value: JsonValue) -> Self {
        Self { value }
    }
}

impl ToolOutput for TeamToolResult {
    fn log_preview(&self) -> String {
        tool_output_json_text(self, "team")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "team")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "team")
    }
}

async fn maybe_record_deviation(invocation: &ToolInvocation, capability: ToolCapability) {
    let team = invocation.session.services.agent_control.team();
    let thread_id = invocation.session.thread_id.to_string();
    let Some(available) = team.available_tools_for(&thread_id) else {
        return;
    };
    if !available.contains(&capability) {
        return;
    }
    let recommended = team.recommended_tools_for(&thread_id).unwrap_or_default();
    if recommended.contains(&capability) {
        return;
    }
    if let Some(binding) = team.binding_snapshot(&thread_id) {
        let _ = team
            .record_deviation(
                &binding.team_session_id,
                &format!(
                    "used {} outside the recommended node surface",
                    capability.as_str()
                ),
            )
            .await;
    }
}
