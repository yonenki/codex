use super::*;
use crate::agent::control::SpawnAgentOptions;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::ACP_ROLE_NAME;
use crate::agent::role::acp_backend;
use crate::agent::role::acp_role_settings;
use crate::agent::status::is_final;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::handlers::multi_agents_common::DEFAULT_WAIT_TIMEOUT_MS;
use crate::tools::handlers::multi_agents_common::MAX_WAIT_TIMEOUT_MS;
use crate::tools::handlers::multi_agents_common::MIN_WAIT_TIMEOUT_MS;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_role;
use crate::tools::handlers::multi_agents_common::apply_spawn_agent_runtime_overrides;
use crate::tools::handlers::multi_agents_common::build_agent_spawn_config;
use crate::tools::handlers::multi_agents_common::collab_spawn_error;
use crate::tools::handlers::multi_agents_common::thread_spawn_source;
use codex_protocol::AgentPath;
use codex_protocol::protocol::AgentStatus;
use codex_protocol::protocol::InterAgentCommunication;
use codex_team_runtime::ExternalWaitCommand;
use std::time::Duration;
use tokio::time::timeout;

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
            run_authorized_team_tool(invocation, capability, |invocation, authority| {
                handle_agent_tool(capability, invocation, authority)
            })
            .await
        })
    }
}

impl CoreToolRuntime for TeamAgentToolHandler {
    fn team_lifecycle_routing(&self) -> TeamLifecycleRouting {
        TeamLifecycleRouting::HandlerOwned
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }

    fn waits_for_runtime_cancellation(&self) -> bool {
        team_authority_class(self.capability) == TeamAuthorityClass::TeamSession
    }
}

#[derive(Debug, Deserialize)]
struct TeamSpawnArgs {
    role: Option<String>,
    agent_type: Option<String>,
    task_name: String,
    message: String,
    fallback_from: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TeamTargetArgs {
    target: String,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TeamWaitArgs {
    target: Option<String>,
    timeout_ms: Option<i64>,
    reason: Option<String>,
    resolve: Option<bool>,
    expected_revision: Option<u64>,
}

async fn handle_agent_tool(
    capability: ToolCapability,
    invocation: ToolInvocation,
    authority: TeamToolAuthority,
) -> Result<TeamToolResult, FunctionCallError> {
    let TeamToolAuthority::TeamSession(team_session_id) = authority else {
        return Err(FunctionCallError::RespondToModel(
            "team agent tool requires Team session authority".into(),
        ));
    };
    match capability {
        ToolCapability::SpawnAgent => handle_team_spawn(invocation, team_session_id).await,
        ToolCapability::SendMessage => {
            handle_team_message(invocation, team_session_id, /*trigger_turn*/ false).await
        }
        ToolCapability::FollowupAgent => {
            handle_team_message(invocation, team_session_id, /*trigger_turn*/ true).await
        }
        ToolCapability::Wait => handle_team_wait(invocation, team_session_id).await,
        ToolCapability::InterruptAgent => handle_team_interrupt(invocation, team_session_id).await,
        ToolCapability::ListAgents => handle_team_list(invocation, team_session_id).await,
        _ => Err(FunctionCallError::RespondToModel(
            "unsupported team agent tool".into(),
        )),
    }
}

async fn handle_team_spawn(
    invocation: ToolInvocation,
    team_session_id: TeamSessionId,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamSpawnArgs = parse_arguments(&arguments)?;
    let role = args
        .role
        .or(args.agent_type)
        .ok_or_else(|| FunctionCallError::RespondToModel("role is required".into()))?;
    let mut pending = invocation
        .session
        .services
        .agent_control
        .team()
        .pending_binding_for_node(&team_session_id, &role)
        .await
        .map_err(map_team_error)?;
    pending.backend_fallback = args
        .fallback_from
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    let message = message_content(args.message)?;
    pending.attach_metadata = Some(codex_team_runtime::PendingAgentAttachMetadata::new(
        message.clone(),
    ));
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
    team_session_id: TeamSessionId,
    trigger_turn: bool,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamTargetArgs = parse_arguments(&arguments)?;
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
        trigger_turn,
    );
    invocation
        .session
        .services
        .agent_control
        .send_inter_agent_communication(
            target,
            communication,
            AgentCommunicationContext::new(
                if trigger_turn {
                    AgentCommunicationKind::Followup
                } else {
                    AgentCommunicationKind::Message
                },
                invocation.session.thread_id,
            ),
            trigger_turn.then(|| invocation.turn.sub_id.clone()),
            invocation.turn.turn_metadata_state.root_turn_id(),
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
    Ok(TeamToolResult::view(view))
}

async fn handle_team_wait(
    invocation: ToolInvocation,
    team_session_id: TeamSessionId,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamWaitArgs = parse_arguments(&arguments)?;
    let team = invocation.session.services.agent_control.team();
    if args.resolve.unwrap_or(false) {
        let view = team
            .resolve_external_wait(ExternalWaitCommand {
                team_session_id,
                reason: args.reason.unwrap_or_else(|| "resolved".into()),
                expected_revision: revision(args.expected_revision)?,
            })
            .await
            .map_err(map_team_error)?;
        return Ok(TeamToolResult::json(serde_json::json!({
            "view": view,
            "resolved": true,
        })));
    }
    if let Some(target) = args.target.filter(|value| !value.trim().is_empty()) {
        let resolved = crate::agent::agent_resolver::resolve_agent_target(
            &invocation.session,
            &invocation.turn,
            &target,
        )
        .await?;
        team.require_same_team(&team_session_id, &resolved.to_string())
            .await
            .map_err(map_team_error)?;
        let target_thread_id = resolved.to_string();
        let reason = args
            .reason
            .unwrap_or_else(|| format!("wait_agent:{target}"));
        team.record_agent_wait_entered(&team_session_id, &target_thread_id, &reason)
            .await
            .map_err(map_team_error)?;
        let timeout_ms = args
            .timeout_ms
            .unwrap_or(DEFAULT_WAIT_TIMEOUT_MS)
            .clamp(MIN_WAIT_TIMEOUT_MS, MAX_WAIT_TIMEOUT_MS);
        let (status, timed_out) =
            wait_for_team_agent(&invocation, resolved, timeout_ms as u64).await?;
        let view = team
            .record_agent_wait_resolved(&team_session_id, &target_thread_id, &reason)
            .await
            .map_err(map_team_error)?;
        return Ok(TeamToolResult::json(serde_json::json!({
            "view": view,
            "target": target,
            "status": status,
            "timed_out": timed_out,
        })));
    }
    let view = team
        .enter_external_wait(ExternalWaitCommand {
            team_session_id,
            reason: args.reason.unwrap_or_else(|| "external".into()),
            expected_revision: revision(args.expected_revision)?,
        })
        .await
        .map_err(map_team_error)?;
    Ok(TeamToolResult::json(serde_json::json!({
        "view": view,
        "waiting": view.waiting_reason,
    })))
}

async fn wait_for_team_agent(
    invocation: &ToolInvocation,
    target: codex_protocol::ThreadId,
    timeout_ms: u64,
) -> Result<(AgentStatus, bool), FunctionCallError> {
    let mut rx = match invocation
        .session
        .services
        .agent_control
        .subscribe_status(target)
        .await
    {
        Ok(rx) => rx,
        Err(_) => return Ok((AgentStatus::NotFound, false)),
    };
    let current = rx.borrow().clone();
    if is_final(&current) {
        return Ok((current, false));
    }
    let wait = async {
        loop {
            if rx.changed().await.is_err() {
                return rx.borrow().clone();
            }
            let status = rx.borrow().clone();
            if is_final(&status) {
                return status;
            }
        }
    };
    match timeout(Duration::from_millis(timeout_ms), wait).await {
        Ok(status) => Ok((status, false)),
        Err(_) => Ok((rx.borrow().clone(), true)),
    }
}

fn revision(value: Option<u64>) -> Result<codex_team_runtime::StateRevision, FunctionCallError> {
    Ok(codex_team_runtime::StateRevision::new(value.ok_or_else(
        || FunctionCallError::RespondToModel("expected_revision is required".into()),
    )?))
}

async fn handle_team_interrupt(
    invocation: ToolInvocation,
    team_session_id: TeamSessionId,
) -> Result<TeamToolResult, FunctionCallError> {
    let arguments = function_arguments(invocation.payload.clone())?;
    let args: TeamTargetArgs = parse_arguments(&arguments)?;
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

async fn handle_team_list(
    invocation: ToolInvocation,
    team_session_id: TeamSessionId,
) -> Result<TeamToolResult, FunctionCallError> {
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
                (
                    "fallback_from".into(),
                    string_prop(
                        "Prior Team-bound agent that failed its backend. Marks this spawn as an explicit backend fallback.",
                    ),
                ),
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
            "Wait for a Team-bound agent, or record and resolve an external wait on the Team trace. Does not change collaboration.wait_agent.",
            BTreeMap::from([
                team_session,
                (
                    "expected_revision".into(),
                    JsonSchema::number(Some(
                        "CAS revision required when entering or resolving an external wait.".into(),
                    )),
                ),
                (
                    "target".into(),
                    string_prop("Optional Team-bound agent target."),
                ),
                (
                    "timeout_ms".into(),
                    JsonSchema::number(Some("Optional timeout when waiting for an agent.".into())),
                ),
                (
                    "reason".into(),
                    string_prop("External wait reason when no agent target is given."),
                ),
                (
                    "resolve".into(),
                    JsonSchema::boolean(Some(
                        "True records ExternalWaitResolved on the Team trace.".into(),
                    )),
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
