use super::*;
use crate::session::session::Session;
use codex_team_graph::discover_team_graphs;
use codex_team_graph::load_known_roles;
use codex_team_runtime::EndTeamCommand;
use codex_team_runtime::RecordResultCommand;
use codex_team_runtime::StartNodeCommand;
use codex_team_runtime::StartTeamCommand;
use codex_team_runtime::StateRevision;
use codex_team_runtime::TransitionCommand;

pub(crate) struct TeamLifecycleToolHandler {
    capability: ToolCapability,
}

impl TeamLifecycleToolHandler {
    pub(crate) fn new(capability: ToolCapability) -> Self {
        Self { capability }
    }
}

impl ToolExecutor<ToolInvocation> for TeamLifecycleToolHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(self.capability.as_str())
    }

    fn spec(&self) -> ToolSpec {
        lifecycle_spec(self.capability)
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        let capability = self.capability;
        Box::pin(async move {
            maybe_record_deviation(&invocation, capability).await;
            handle_lifecycle(capability, invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for TeamLifecycleToolHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct GraphNameArgs {
    graph_name: String,
}

#[derive(Debug, Deserialize)]
struct TeamSessionArgs {
    team_session_id: Option<String>,
    expected_revision: Option<u64>,
    node_id: Option<String>,
    graph_name: Option<String>,
    task_ref: Option<String>,
    worktree: Option<String>,
    branch: Option<String>,
    result: Option<String>,
    evidence_id: Option<String>,
    candidate_sha: Option<String>,
    deviation_reason: Option<String>,
    aborted: Option<bool>,
    reason: Option<String>,
}

async fn handle_lifecycle(
    capability: ToolCapability,
    invocation: ToolInvocation,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    refresh_catalog(&invocation.session, invocation.turn.as_ref()).await;
    let team = invocation.session.services.agent_control.team();
    match capability {
        ToolCapability::ListTeamGraphs => {
            let graphs = team.list_graphs().await;
            return Ok(TeamToolResult::json(serde_json::json!({
                "graphs": graphs,
                "revision": 0,
                "possible_next": [{"tool": "get_team_graph", "reason": "Inspect a discovered graph."}],
                "recommended_next": [{"tool": "start_team", "reason": "Start a team from a discovered graph."}],
            })));
        }
        ToolCapability::GetTeamGraph => {
            let args: GraphNameArgs = parse_arguments(&arguments)?;
            let graph = team
                .get_graph(&args.graph_name)
                .await
                .map_err(map_team_error)?;
            let start = graph
                .node(&graph.start)
                .map(codex_team_graph::NodeGuide::from_node);
            return Ok(TeamToolResult::json(serde_json::json!({
                "graph": graph.summary(),
                "start": start,
                "revision": 0,
                "possible_next": [{"tool": "start_team", "reason": "Start this graph."}],
                "recommended_next": [{"tool": "start_team", "reason": "Start this graph."}],
            })));
        }
        ToolCapability::ListTeams => {
            let teams = team.list_teams().await;
            return Ok(TeamToolResult::json(serde_json::json!({
                "teams": teams,
                "revision": 0,
                "possible_next": [{"tool": "get_team_status", "reason": "Inspect one team_session_id."}],
                "recommended_next": [{"tool": "get_team_status", "reason": "Inspect one team_session_id."}],
            })));
        }
        _ => {}
    }
    let args: TeamSessionArgs = parse_arguments(&arguments).unwrap_or(TeamSessionArgs {
        team_session_id: None,
        expected_revision: None,
        node_id: None,
        graph_name: None,
        task_ref: None,
        worktree: None,
        branch: None,
        result: None,
        evidence_id: None,
        candidate_sha: None,
        deviation_reason: None,
        aborted: None,
        reason: None,
    });
    let view = match capability {
        ToolCapability::StartTeam => {
            let graph_name = args.graph_name.ok_or_else(|| {
                FunctionCallError::RespondToModel("graph_name is required".into())
            })?;
            team.start_team(StartTeamCommand {
                graph_name,
                task_ref: args.task_ref,
                worktree: args.worktree,
                branch: args.branch,
            })
            .await
            .map_err(map_team_error)?
        }
        ToolCapability::GetTeamStatus | ToolCapability::GetTeamNext => {
            let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
            team.status(&team_session_id)
                .await
                .map_err(map_team_error)?
        }
        ToolCapability::StartTeamNode => {
            let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
            team.start_node(StartNodeCommand {
                team_session_id,
                node_id: args.node_id,
                expected_revision: revision(args.expected_revision)?,
            })
            .await
            .map_err(map_team_error)?
        }
        ToolCapability::RecordTeamResult => {
            let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
            team.record_result(RecordResultCommand {
                team_session_id,
                result: args.result.ok_or_else(|| {
                    FunctionCallError::RespondToModel("result is required".into())
                })?,
                evidence_id: args.evidence_id,
                candidate_sha: args.candidate_sha,
                expected_revision: revision(args.expected_revision)?,
            })
            .await
            .map_err(map_team_error)?
        }
        ToolCapability::TransitionTeam => {
            let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
            team.transition(TransitionCommand {
                team_session_id,
                result: args.result.ok_or_else(|| {
                    FunctionCallError::RespondToModel("result is required".into())
                })?,
                deviation_reason: args.deviation_reason,
                expected_revision: revision(args.expected_revision)?,
            })
            .await
            .map_err(map_team_error)?
        }
        ToolCapability::EndTeam => {
            let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
            team.end_team(EndTeamCommand {
                team_session_id,
                aborted: args.aborted.unwrap_or(false),
                reason: args.reason.unwrap_or_else(|| "completed".into()),
                expected_revision: revision(args.expected_revision)?,
            })
            .await
            .map_err(map_team_error)?
        }
        _ => {
            return Err(FunctionCallError::RespondToModel(
                "unsupported team lifecycle tool".into(),
            ));
        }
    };
    Ok(TeamToolResult::view(view))
}

fn revision(value: Option<u64>) -> Result<StateRevision, FunctionCallError> {
    Ok(StateRevision::new(value.ok_or_else(|| {
        FunctionCallError::RespondToModel("expected_revision is required".into())
    })?))
}

async fn refresh_catalog(session: &Session, turn: &crate::session::turn_context::TurnContext) {
    #[allow(deprecated)]
    let cwd = turn.cwd.as_path();
    let roles = load_known_roles(cwd, turn.config.agent_roles.keys().cloned());
    if let Ok(catalog) = discover_team_graphs(cwd, Some(turn.config.codex_home.as_path()), &roles) {
        session
            .services
            .agent_control
            .team()
            .replace_catalog(catalog)
            .await;
    }
}

fn lifecycle_spec(capability: ToolCapability) -> ToolSpec {
    let team_session = (
        "team_session_id".to_string(),
        string_prop("Team session id. Required for the unbound root coordinator."),
    );
    let revision = (
        "expected_revision".to_string(),
        JsonSchema::number(Some("CAS revision from the last team status.".into())),
    );
    match capability {
        ToolCapability::ListTeamGraphs => object_spec(
            capability.as_str(),
            "List discovered Team Graphs from .codex/teams.",
            BTreeMap::new(),
            Vec::new(),
        ),
        ToolCapability::GetTeamGraph => object_spec(
            capability.as_str(),
            "Load one Team Graph entry and its start-node guide.",
            BTreeMap::from([("graph_name".into(), string_prop("Graph name."))]),
            vec!["graph_name".into()],
        ),
        ToolCapability::ListTeams => object_spec(
            capability.as_str(),
            "List Team sessions owned by this root coordinator.",
            BTreeMap::new(),
            Vec::new(),
        ),
        ToolCapability::StartTeam => object_spec(
            capability.as_str(),
            "Start a Team session from a discovered graph.",
            BTreeMap::from([
                ("graph_name".into(), string_prop("Graph name.")),
                ("task_ref".into(), string_prop("Issue or PR reference.")),
                ("worktree".into(), string_prop("Worktree path.")),
                ("branch".into(), string_prop("Branch name.")),
            ]),
            vec!["graph_name".into()],
        ),
        ToolCapability::GetTeamStatus | ToolCapability::GetTeamNext => object_spec(
            capability.as_str(),
            "Return the current node guide, revision, and recommended next tools.",
            BTreeMap::from([team_session]),
            vec!["team_session_id".into()],
        ),
        ToolCapability::StartTeamNode => object_spec(
            capability.as_str(),
            "Start a node run for the selected Team.",
            BTreeMap::from([
                team_session,
                revision,
                (
                    "node_id".into(),
                    string_prop("Optional node id. Defaults to the current node."),
                ),
            ]),
            vec!["team_session_id".into(), "expected_revision".into()],
        ),
        ToolCapability::RecordTeamResult => object_spec(
            capability.as_str(),
            "Record a structured node result and optional evidence identity.",
            BTreeMap::from([
                team_session,
                revision,
                ("result".into(), string_prop("Structured result name.")),
                ("evidence_id".into(), string_prop("Optional evidence id.")),
                (
                    "candidate_sha".into(),
                    string_prop("Optional candidate SHA."),
                ),
            ]),
            vec![
                "team_session_id".into(),
                "expected_revision".into(),
                "result".into(),
            ],
        ),
        ToolCapability::TransitionTeam => object_spec(
            capability.as_str(),
            "Select a declared transition. Non-recommended transitions require deviation_reason.",
            BTreeMap::from([
                team_session,
                revision,
                ("result".into(), string_prop("Transition result name.")),
                (
                    "deviation_reason".into(),
                    string_prop("Required when the transition is not recommended."),
                ),
            ]),
            vec![
                "team_session_id".into(),
                "expected_revision".into(),
                "result".into(),
            ],
        ),
        ToolCapability::EndTeam => object_spec(
            capability.as_str(),
            "Close a Team session as completed or aborted.",
            BTreeMap::from([
                team_session,
                revision,
                (
                    "aborted".into(),
                    JsonSchema::boolean(Some("True aborts the team.".into())),
                ),
                ("reason".into(), string_prop("Completion or abort reason.")),
            ]),
            vec!["team_session_id".into(), "expected_revision".into()],
        ),
        _ => object_spec(
            capability.as_str(),
            "Team tool.",
            BTreeMap::new(),
            Vec::new(),
        ),
    }
}
