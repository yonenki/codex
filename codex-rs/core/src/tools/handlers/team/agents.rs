use super::*;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::ACP_ROLE_NAME;
use crate::agent::role::acp_backend;
use crate::agent::role::acp_role_settings;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_role;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_runtime_overrides;
use crate::tools::handlers::multi_agents_common::build_agent_spawn_config;
use crate::tools::handlers::multi_agents_common::collab_spawn_error;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;
use crate::tools::handlers::multi_agents_v2::wait as wait_handler;
use codex_protocol::AgentPath;
use codex_protocol::protocol::InterAgentCommunication;

fn message_content(message: String) -> Result<String, FunctionCallError> {
    if message.trim().is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "Empty message can't be sent to an agent".to_string(),
        ));
    }
    Ok(message)
}

pub(crate) struct TeamAgentToolHandler {
    capability: ToolCapability,
}

impl TeamAgentToolHandler {
    pub(crate) fn new(capability: ToolCapability) -> Self {
        Self { capability }
    }
}

impl ToolExecutor<ToolInvocation> for TeamAgentToolHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain(self.capability.as_str())
    }

    fn spec(&self) -> ToolSpec {
        agent_spec(self.capability)
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        let capability = self.capability;
        Box::pin(async move {
            maybe_record_deviation(&invocation, capability).await;
            handle_agent_tool(capability, invocation)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for TeamAgentToolHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
struct TeamSpawnArgs {
    team_session_id: Option<String>,
    role: Option<String>,
    agent_type: Option<String>,
    task_name: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct TeamTargetArgs {
    team_session_id: Option<String>,
    target: String,
    message: Option<String>,
    timeout_ms: Option<i64>,
}

async fn handle_agent_tool(
    capability: ToolCapability,
    invocation: ToolInvocation,
) -> Result<TeamToolResult, FunctionCallError> {
    match capability {
        ToolCapability::SpawnAgent => handle_team_spawn(invocation).await,
        ToolCapability::SendMessage => {
            handle_team_message(invocation, /*trigger_turn*/ false).await
        }
        ToolCapability::FollowupAgent => {
            handle_team_message(invocation, /*trigger_turn*/ true).await
        }
        ToolCapability::Wait => handle_team_wait(invocation).await,
        ToolCapability::InterruptAgent => handle_team_interrupt(invocation).await,
        ToolCapability::ListAgents => handle_team_list(invocation).await,
        _ => Err(FunctionCallError::RespondToModel(
            "unsupported team agent tool".into(),
        )),
    }
}

async fn handle_team_spawn(
    invocation: ToolInvocation,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamSpawnArgs = parse_arguments(&arguments)?;
    let role = args
        .role
        .or(args.agent_type)
        .ok_or_else(|| FunctionCallError::RespondToModel("role is required".into()))?;
    let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
    let pending = invocation
        .session
        .services
        .agent_control
        .team()
        .pending_binding_for_node(&team_session_id, &role)
        .await
        .map_err(map_team_error)?;
    let message = message_content(args.message)?;
    let turn = invocation.turn.as_ref();
    let mut config = build_agent_spawn_config(
        &invocation.session.get_base_instructions().await,
        turn,
        invocation.step_context.environments.primary(),
    )?;
    apply_spawn_agent_runtime_overrides(
        &mut config,
        turn,
        invocation.step_context.environments.primary(),
    )?;
    let acp = acp_role_settings(&config, &role)
        .await
        .map_err(FunctionCallError::RespondToModel)?;
    if acp.backends.is_empty() {
        apply_spawn_agent_role(&invocation.session, &mut config, Some(&role)).await?;
        let spawn_source = thread_spawn_source(
            invocation.session.thread_id,
            &turn.session_source,
            next_thread_spawn_depth(&turn.session_source),
            Some(&role),
            Some(args.task_name.clone()),
        )?;
        let new_agent_path = spawn_source.get_agent_path().ok_or_else(|| {
            FunctionCallError::RespondToModel("spawned agent is missing a task name".into())
        })?;
        let communication = InterAgentCommunication::new(
            turn.session_source
                .get_agent_path()
                .unwrap_or_else(AgentPath::root),
            new_agent_path.clone(),
            Vec::new(),
            message,
            /*trigger_turn*/ true,
        );
        let spawned = invocation
            .session
            .services
            .agent_control
            .spawn_agent_with_communication(
                config,
                communication,
                AgentCommunicationContext::new(
                    AgentCommunicationKind::Spawn,
                    invocation.session.thread_id,
                ),
                Some(spawn_source),
                SpawnAgentOptions {
                    parent_thread_id: Some(invocation.session.thread_id),
                    parent_turn_id: Some(turn.sub_id.clone()),
                    root_turn_id: turn.turn_metadata_state.root_turn_id(),
                    environments: Some(invocation.step_context.environments.to_selections()),
                    pending_team_binding: Some(pending),
                    ..SpawnAgentOptions::default()
                },
            )
            .await
            .map_err(collab_spawn_error)?;
        let view = invocation
            .session
            .services
            .agent_control
            .team()
            .status(&team_session_id)
            .await
            .map_err(map_team_error)?;
        return Ok(TeamToolResult::json(serde_json::json!({
            "task_name": String::from(new_agent_path),
            "agent_thread_id": spawned.thread_id.to_string(),
            "backend": "native",
            "view": view,
        })));
    }

    let backend = acp.backends[0].clone();
    let spawn_source = thread_spawn_source(
        invocation.session.thread_id,
        &turn.session_source,
        next_thread_spawn_depth(&turn.session_source),
        Some(&role),
        Some(args.task_name),
    )?;
    let agent_path = spawn_source.get_agent_path().ok_or_else(|| {
        FunctionCallError::RespondToModel("spawned ACP agent is missing a task name".into())
    })?;
    let communication = InterAgentCommunication::new(
        turn.session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root),
        agent_path.clone(),
        Vec::new(),
        message,
        /*trigger_turn*/ true,
    );
    let harness = backend.harness.clone();
    let model = backend.model.clone();
    let spawned = invocation
        .session
        .services
        .agent_control
        .spawn_external_agent_with_communication(
            config,
            acp_backend(harness.clone(), model.clone(), backend.effort),
            communication,
            AgentCommunicationContext::new(
                AgentCommunicationKind::Spawn,
                invocation.session.thread_id,
            ),
            spawn_source,
            None,
            Some(pending),
            |_| async {},
        )
        .await
        .map_err(collab_spawn_error)?;
    let _ = ACP_ROLE_NAME;
    let view = invocation
        .session
        .services
        .agent_control
        .team()
        .status(&team_session_id)
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::json(serde_json::json!({
        "task_name": String::from(agent_path),
        "agent_thread_id": spawned.thread_id.to_string(),
        "backend": "acp",
        "harness": harness,
        "model": model,
        "view": view,
    })))
}

async fn handle_team_message(
    invocation: ToolInvocation,
    trigger_turn: bool,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamTargetArgs = parse_arguments(&arguments)?;
    let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
    let target = crate::agent::agent_resolver::resolve_agent_target(
        &invocation.session,
        &invocation.turn,
        &args.target,
    )
    .await?;
    invocation
        .session
        .services
        .agent_control
        .team()
        .require_same_team(&team_session_id, &target.to_string())
        .await
        .map_err(map_team_error)?;
    let message = message_content(args.message.unwrap_or_default())?;
    if trigger_turn {
        invocation
            .session
            .services
            .agent_control
            .send_input(
                target,
                vec![codex_protocol::user_input::UserInput::Text {
                    text: message,
                    text_elements: Vec::new(),
                }],
                Some(invocation.turn.sub_id.clone()),
                invocation.turn.turn_metadata_state.root_turn_id(),
            )
            .await
            .map_err(collab_spawn_error)?;
    } else {
        let communication = InterAgentCommunication::new(
            invocation
                .turn
                .session_source
                .get_agent_path()
                .unwrap_or_else(AgentPath::root),
            invocation
                .session
                .services
                .agent_control
                .ensure_agent_known(target)
                .ok()
                .and_then(|agent| agent.agent_path)
                .unwrap_or_else(AgentPath::root),
            Vec::new(),
            message,
            /*trigger_turn*/ false,
        );
        invocation
            .session
            .services
            .agent_control
            .send_inter_agent_communication(
                target,
                communication,
                AgentCommunicationContext::new(
                    AgentCommunicationKind::Message,
                    invocation.session.thread_id,
                ),
                Some(invocation.turn.sub_id.clone()),
                invocation.turn.turn_metadata_state.root_turn_id(),
            )
            .await
            .map_err(collab_spawn_error)?;
    }
    let view = invocation
        .session
        .services
        .agent_control
        .team()
        .status(&team_session_id)
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::view(view))
}

async fn handle_team_wait(invocation: ToolInvocation) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamTargetArgs = parse_arguments(&arguments)?;
    let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
    let _ = args.timeout_ms;
    let _ = wait_handler::Handler::new(Default::default());
    let view = invocation
        .session
        .services
        .agent_control
        .team()
        .status(&team_session_id)
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::json(serde_json::json!({
        "view": view,
        "wait": "use collaboration.wait_agent for the existing wait contract; team.wait records Team-scoped status",
    })))
}

async fn handle_team_interrupt(
    invocation: ToolInvocation,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamTargetArgs = parse_arguments(&arguments)?;
    let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
    let target = crate::agent::agent_resolver::resolve_agent_target(
        &invocation.session,
        &invocation.turn,
        &args.target,
    )
    .await?;
    invocation
        .session
        .services
        .agent_control
        .team()
        .require_same_team(&team_session_id, &target.to_string())
        .await
        .map_err(map_team_error)?;
    invocation
        .session
        .services
        .agent_control
        .interrupt_agent(target)
        .await
        .map_err(collab_spawn_error)?;
    invocation
        .session
        .services
        .agent_control
        .team()
        .record_agent_terminal(&target.to_string(), "interrupted")
        .await
        .map_err(map_team_error)?;
    let view = invocation
        .session
        .services
        .agent_control
        .team()
        .status(&team_session_id)
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::view(view))
}

async fn handle_team_list(invocation: ToolInvocation) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamSessionArgsLite = parse_arguments(&arguments).unwrap_or(TeamSessionArgsLite {
        team_session_id: None,
    });
    let team_session_id = require_team_session_id(&invocation, args.team_session_id)?;
    let view = invocation
        .session
        .services
        .agent_control
        .team()
        .status(&team_session_id)
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::json(serde_json::json!({
        "agents": view.agents,
        "revision": view.revision,
        "possible_next": view.possible_next,
        "recommended_next": view.recommended_next,
        "guide": view.current_node,
    })))
}

#[derive(Debug, Deserialize)]
struct TeamSessionArgsLite {
    team_session_id: Option<String>,
}

fn agent_spec(capability: ToolCapability) -> ToolSpec {
    let team_session = (
        "team_session_id".to_string(),
        string_prop("Team session id. Required for the unbound root coordinator."),
    );
    match capability {
        ToolCapability::SpawnAgent => object_spec(
            capability.as_str(),
            "Spawn a Team-bound agent. Role selects Native or ACP internally. Do not pass harness, model, or effort.",
            BTreeMap::from([
                team_session,
                (
                    "role".into(),
                    string_prop("Existing .codex/agents Role name."),
                ),
                ("agent_type".into(), string_prop("Alias of role.")),
                (
                    "task_name".into(),
                    string_prop("Task name for the spawned agent."),
                ),
                ("message".into(), string_prop("Initial task message.")),
            ]),
            vec!["task_name".into(), "message".into()],
        ),
        ToolCapability::SendMessage | ToolCapability::FollowupAgent => object_spec(
            capability.as_str(),
            "Send a Team-scoped message to a bound agent.",
            BTreeMap::from([
                team_session,
                (
                    "target".into(),
                    string_prop("Agent task name or thread id."),
                ),
                ("message".into(), string_prop("Message body.")),
            ]),
            vec!["target".into(), "message".into()],
        ),
        ToolCapability::Wait => object_spec(
            capability.as_str(),
            "Inspect Team-scoped wait state. Use collaboration.wait_agent for the existing wait contract.",
            BTreeMap::from([
                team_session,
                ("target".into(), string_prop("Optional agent target.")),
                (
                    "timeout_ms".into(),
                    JsonSchema::number(Some("Optional timeout.".into())),
                ),
            ]),
            vec!["team_session_id".into()],
        ),
        ToolCapability::InterruptAgent => object_spec(
            capability.as_str(),
            "Interrupt a Team-bound agent.",
            BTreeMap::from([
                team_session,
                (
                    "target".into(),
                    string_prop("Agent task name or thread id."),
                ),
            ]),
            vec!["target".into()],
        ),
        ToolCapability::ListAgents => object_spec(
            capability.as_str(),
            "List agents bound to one Team session.",
            BTreeMap::from([team_session]),
            vec!["team_session_id".into()],
        ),
        _ => object_spec(
            capability.as_str(),
            "Team agent tool.",
            BTreeMap::new(),
            Vec::new(),
        ),
    }
}
