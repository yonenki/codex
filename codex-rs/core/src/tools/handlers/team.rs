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
use crate::tools::registry::TeamLifecycleRouting;
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
use std::future::Future;

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

#[derive(Clone, Debug)]
enum TeamToolAuthority {
    CatalogRead,
    ListTeams { bound_team: Option<TeamSessionId> },
    StartTeam,
    TeamSession(TeamSessionId),
}

impl TeamToolAuthority {
    fn scoped_team(&self) -> Option<&TeamSessionId> {
        match self {
            Self::TeamSession(team_session_id) => Some(team_session_id),
            Self::CatalogRead | Self::ListTeams { .. } | Self::StartTeam => None,
        }
    }
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
    use std::sync::Arc;

    #[derive(Default)]
    struct FailFailedLifecycleStore {
        inner: codex_team_runtime::MemoryTeamStore,
    }

    impl codex_team_runtime::TeamStore for FailFailedLifecycleStore {
        async fn persist_event(
            &self,
            state: &codex_team_runtime::TeamSessionState,
            event: &codex_team_runtime::TeamEvent,
        ) -> codex_team_runtime::TeamRuntimeResult<()> {
            if event.kind == codex_team_runtime::TeamEventKind::ToolOperationFailed {
                return Err(codex_team_runtime::TeamRuntimeError::Store(
                    "terminal trace unavailable".to_string(),
                ));
            }
            self.inner.persist_event(state, event).await
        }

        async fn load_teams(
            &self,
        ) -> codex_team_runtime::TeamRuntimeResult<Vec<codex_team_runtime::TeamSessionState>>
        {
            self.inner.load_teams().await
        }

        async fn load_events(
            &self,
            team_session_id: &codex_team_runtime::TeamSessionId,
        ) -> codex_team_runtime::TeamRuntimeResult<Vec<codex_team_runtime::TeamEvent>> {
            self.inner.load_events(team_session_id).await
        }

        async fn pending_outbox(
            &self,
        ) -> codex_team_runtime::TeamRuntimeResult<Vec<codex_team_runtime::TeamEvent>> {
            self.inner.pending_outbox().await
        }

        async fn mark_outbox_sent(
            &self,
            event_ids: &[codex_team_runtime::EventId],
        ) -> codex_team_runtime::TeamRuntimeResult<()> {
            self.inner.mark_outbox_sent(event_ids).await
        }

        async fn persist_binding(
            &self,
            binding: &codex_team_runtime::TeamAgentBinding,
        ) -> codex_team_runtime::TeamRuntimeResult<()> {
            self.inner.persist_binding(binding).await
        }

        async fn load_bindings(
            &self,
        ) -> codex_team_runtime::TeamRuntimeResult<Vec<codex_team_runtime::TeamAgentBinding>>
        {
            self.inner.load_bindings().await
        }
    }

    fn team(id: &str) -> TeamSessionId {
        TeamSessionId::parse(id).expect("team id")
    }

    fn lifecycle_graph() -> codex_team_graph::TeamGraph {
        let dto: codex_team_graph::TeamGraphToml = toml::from_str(
            r#"
schema_version = 1
name = "start-trace"
version = "1"
description = "Start lifecycle trace."
start = "work"
terminals = ["done"]
[[nodes]]
id = "work"
purpose = "Work."
role = "worker"
prompt = "Work."
completion = "Done."
available_tools = ["record_team_result"]
recommended_tools = ["record_team_result"]
[[nodes.transitions]]
on = "done"
to = "done"
recommended = true
guide = "Finish."
[[nodes]]
id = "done"
purpose = "Done."
prompt = "Stop."
completion = "Closed."
"#,
        )
        .expect("graph TOML");
        let graph = codex_team_graph::TeamGraph::try_from(dto).expect("graph");
        codex_team_graph::validate_team_graph(
            &graph,
            &["worker".to_string()].into_iter().collect(),
        )
        .expect("valid graph");
        graph
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

    #[tokio::test]
    async fn start_team_records_created_team_lifecycle_once_in_sequence() {
        let graph = lifecycle_graph();
        let sink = codex_team_runtime::RecordingSink::default();
        let team = Arc::new(codex_team_runtime::TeamControl::with_memory_store(
            codex_team_graph::TeamGraphCatalog::new([graph]),
            sink.clone(),
        ));
        let (mut session, turn) = crate::session::tests::make_session_and_context().await;
        session.services.agent_control =
            crate::agent::control::AgentControl::with_team_for_tests(team);
        let session = Arc::new(session);
        let turn = Arc::new(turn);
        let step_context = crate::session::step_context::StepContext::for_test(Arc::clone(&turn));
        let invocation = ToolInvocation {
            session,
            turn,
            step_context,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(tokio::sync::Mutex::new(
                crate::turn_diff_tracker::TurnDiffTracker::default(),
            )),
            call_id: "start-call".to_string(),
            tool_name: ToolName::namespaced("team", ToolCapability::StartTeam.as_str()),
            source: crate::tools::context::ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({"graph_name": "start-trace"}).to_string(),
            },
        };

        run_authorized_team_tool(
            invocation,
            ToolCapability::StartTeam,
            |invocation, _authority| async move {
                let view = invocation
                    .session
                    .services
                    .agent_control
                    .team()
                    .start_team(codex_team_runtime::StartTeamCommand {
                        graph_name: "start-trace".to_string(),
                        task_ref: None,
                        worktree: None,
                        branch: None,
                    })
                    .await
                    .map_err(map_team_error)?;
                Ok(TeamToolResult::view(view))
            },
        )
        .await
        .expect("start Team through lifecycle wrapper");

        let events = sink.envelopes();
        assert_eq!(
            events
                .iter()
                .map(|event| (event.sequence, event.kind.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (1, "team_started"),
                (2, "tool_operation_started"),
                (3, "tool_operation_completed"),
            ]
        );
        assert_eq!(
            events[1].team_session_id, events[0].team_session_id,
            "start lifecycle belongs to the created Team"
        );
        assert_eq!(events[2].team_session_id, events[0].team_session_id);
    }

    #[tokio::test]
    async fn handler_error_wins_when_failed_lifecycle_cannot_persist() {
        let catalog = codex_team_graph::TeamGraphCatalog::new([lifecycle_graph()]);
        let team = Arc::new(codex_team_runtime::TeamControl::with_store(
            catalog,
            FailFailedLifecycleStore::default(),
            codex_team_runtime::RecordingSink::default(),
        ));
        let view = team
            .start_team(codex_team_runtime::StartTeamCommand {
                graph_name: "start-trace".to_string(),
                task_ref: None,
                worktree: None,
                branch: None,
            })
            .await
            .expect("seed Team");
        let (mut session, turn) = crate::session::tests::make_session_and_context().await;
        session.services.agent_control =
            crate::agent::control::AgentControl::with_team_for_tests(team);
        let session = Arc::new(session);
        let turn = Arc::new(turn);
        let step_context = crate::session::step_context::StepContext::for_test(Arc::clone(&turn));
        let invocation = ToolInvocation {
            session,
            turn,
            step_context,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            tracker: Arc::new(tokio::sync::Mutex::new(
                crate::turn_diff_tracker::TurnDiffTracker::default(),
            )),
            call_id: "failed-call".to_string(),
            tool_name: ToolName::namespaced("team", ToolCapability::SendMessage.as_str()),
            source: crate::tools::context::ToolCallSource::Direct,
            payload: ToolPayload::Function {
                arguments: serde_json::json!({
                    "team_session_id": view.team_session_id,
                    "agent_id": "target",
                    "message": "hello"
                })
                .to_string(),
            },
        };

        let result = run_authorized_team_tool(
            invocation,
            ToolCapability::SendMessage,
            |_invocation, _authority| async {
                Err::<TeamToolResult, _>(FunctionCallError::RespondToModel(
                    "original handler failure".to_string(),
                ))
            },
        )
        .await;
        let error = match result {
            Ok(_) => panic!("handler failure must propagate"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            FunctionCallError::RespondToModel(message) if message == "original handler failure"
        ));
    }
}

fn map_team_error(err: codex_team_runtime::TeamRuntimeError) -> FunctionCallError {
    FunctionCallError::RespondToModel(err.to_string())
}

async fn run_authorized_team_tool<F, Fut>(
    invocation: ToolInvocation,
    capability: ToolCapability,
    handler: F,
) -> Result<Box<dyn ToolOutput>, FunctionCallError>
where
    F: FnOnce(ToolInvocation, TeamToolAuthority) -> Fut,
    Fut: Future<Output = Result<TeamToolResult, FunctionCallError>>,
{
    let authority = authorize_team_tool(&invocation, capability).await?;
    let scoped_team = authority.scoped_team().cloned();
    maybe_record_deviation(&invocation, capability).await;
    if let Some(team_session_id) = scoped_team.as_ref() {
        record_team_tool_lifecycle(
            &invocation,
            team_session_id,
            codex_team_runtime::TeamEventKind::ToolOperationStarted,
        )
        .await?;
    }

    let result = handler(invocation.clone(), authority).await;
    let terminal_team = scoped_team.or_else(|| {
        result
            .as_ref()
            .ok()
            .and_then(TeamToolResult::trace_team_session_id)
            .cloned()
    });
    match result {
        Ok(output) => {
            if let Some(team_session_id) = terminal_team {
                if capability == ToolCapability::StartTeam {
                    record_team_tool_lifecycle(
                        &invocation,
                        &team_session_id,
                        codex_team_runtime::TeamEventKind::ToolOperationStarted,
                    )
                    .await?;
                }
                record_team_tool_lifecycle(
                    &invocation,
                    &team_session_id,
                    codex_team_runtime::TeamEventKind::ToolOperationCompleted,
                )
                .await?;
            }
            Ok(boxed_tool_output(output))
        }
        Err(error) => {
            if let Some(team_session_id) = terminal_team {
                // The operation error is authoritative even when its terminal trace cannot persist.
                let _ = record_team_tool_lifecycle(
                    &invocation,
                    &team_session_id,
                    codex_team_runtime::TeamEventKind::ToolOperationFailed,
                )
                .await;
            }
            Err(error)
        }
    }
}

async fn record_team_tool_lifecycle(
    invocation: &ToolInvocation,
    team_session_id: &TeamSessionId,
    kind: codex_team_runtime::TeamEventKind,
) -> Result<(), FunctionCallError> {
    invocation
        .session
        .services
        .agent_control
        .team()
        .record_tool_operation_for_team(
            team_session_id,
            &invocation.session.thread_id.to_string(),
            &invocation.tool_name.to_string(),
            &invocation.call_id,
            kind,
            None,
        )
        .await
        .map_err(map_team_error)
}

#[derive(Debug, Serialize)]
struct TeamToolResult {
    #[serde(flatten)]
    value: JsonValue,
    #[serde(skip)]
    trace_team_session_id: Option<TeamSessionId>,
}

impl TeamToolResult {
    fn view(view: TeamView) -> Self {
        let trace_team_session_id = Some(view.team_session_id.clone());
        Self {
            value: serde_json::to_value(view).unwrap_or(JsonValue::Null),
            trace_team_session_id,
        }
    }

    fn json(value: JsonValue) -> Self {
        Self {
            value,
            trace_team_session_id: None,
        }
    }

    fn trace_team_session_id(&self) -> Option<&TeamSessionId> {
        self.trace_team_session_id.as_ref()
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
