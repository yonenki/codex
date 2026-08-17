use super::*;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::ACP_ROLE_NAME;
use crate::agent::role::AcpRoleSettings;
use crate::agent::role::acp_backend;
use crate::agent::role::acp_role_settings;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::context::FunctionToolOutput;
use crate::tools::handlers::multi_agents_v2::message_tool::message_content;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use serde_json::json;
use std::collections::BTreeMap;

pub(crate) struct SpawnHandler;
pub(crate) struct MessageHandler;
pub(crate) struct FollowupHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnArgs {
    task_name: String,
    message: String,
    harness: Option<String>,
    model: Option<String>,
    effort: Option<String>,
    agent_type: Option<String>,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct AcpBackendOverrides {
    harness: Option<String>,
    model: Option<String>,
    effort: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MessageArgs {
    target: String,
    message: String,
}

#[derive(Clone, Copy)]
enum DeliveryMode {
    QueueOnly,
    TriggerTurn,
}

impl DeliveryMode {
    fn trigger_turn(self) -> bool {
        matches!(self, Self::TriggerTurn)
    }

    fn communication_kind(self) -> AgentCommunicationKind {
        match self {
            Self::QueueOnly => AgentCommunicationKind::Message,
            Self::TriggerTurn => AgentCommunicationKind::Followup,
        }
    }
}

fn spawn_spec() -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "task_name".to_string(),
            JsonSchema::string(Some(
                "Task name for the ACP agent. Use lowercase letters, digits, and underscores."
                    .to_string(),
            )),
        ),
        (
            "harness".to_string(),
            harness_schema(),
        ),
        (
            "effort".to_string(),
            JsonSchema::string(Some(
                "Optional reasoning effort selected through the harness ACP thought-level configuration. The harness default is used when omitted."
                    .to_string(),
            )),
        ),
        (
            "agent_type".to_string(),
            JsonSchema::string(Some(
                "Optional Codex agent role from the active .codex/agents definitions. A role may provide default ACP harness, model, and reasoning effort values; explicit spawn arguments override those defaults."
                    .to_string(),
            )),
        ),
        (
            "model".to_string(),
            JsonSchema::string(Some(
                "Optional model ID selected through the harness ACP session configuration. The harness default is used when omitted."
                    .to_string(),
            )),
        ),
        (
            "message".to_string(),
            JsonSchema::string(Some(
                "Complete plain-text task for the ACP agent. No parent conversation history is inherited."
                    .to_string(),
            )),
        ),
    ]);
    ToolSpec::Function(ResponsesApiTool {
        name: "spawn".to_string(),
        description: concat!(
            "Spawn a registered Agent Client Protocol (ACP) harness as a subagent owned by ",
            "the Codex agent-control plane. The external harness receives the current working ",
            "directory and ACP permission requests are approved once, so it can modify the ",
            "workspace outside Codex sandbox enforcement. Use only when that execution is ",
            "authorized. list_agents, wait_agent, and interrupt_agent use the normal ",
            "collaboration tools."
        )
        .to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["task_name".to_string(), "message".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

fn harness_schema() -> JsonSchema {
    let description =
        Some("Harness ID resolved by the configured external ACP harness host.".to_string());
    let harnesses = std::env::var("CODEX_ACP_HARNESS_IDS_JSON")
        .ok()
        .and_then(|value| serde_json::from_str::<Vec<String>>(&value).ok())
        .unwrap_or_default()
        .into_iter()
        .filter(|harness| is_valid_harness_id(harness))
        .map(|harness| json!(harness))
        .collect::<Vec<_>>();
    if harnesses.is_empty() {
        JsonSchema::string(description)
    } else {
        JsonSchema::string_enum(harnesses, description)
    }
}

fn message_spec(name: &str, description: &str) -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "target".to_string(),
            JsonSchema::string(Some(
                "Canonical or relative task name returned by acp.spawn.".to_string(),
            )),
        ),
        (
            "message".to_string(),
            JsonSchema::string(Some(
                "Plain-text message passed to the target ACP session.".to_string(),
            )),
        ),
    ]);
    ToolSpec::Function(ResponsesApiTool {
        name: name.to_string(),
        description: description.to_string(),
        strict: false,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["target".to_string(), "message".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

impl ToolExecutor<ToolInvocation> for SpawnHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("spawn")
    }

    fn spec(&self) -> ToolSpec {
        spawn_spec()
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move { spawn(invocation).await.map(boxed_tool_output) })
    }
}

impl CoreToolRuntime for SpawnHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

impl ToolExecutor<ToolInvocation> for MessageHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("send_message")
    }

    fn spec(&self) -> ToolSpec {
        message_spec(
            "send_message",
            "Queue a plain-text message for an existing ACP agent without starting a turn.",
        )
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            deliver(invocation, DeliveryMode::QueueOnly)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for MessageHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

impl ToolExecutor<ToolInvocation> for FollowupHandler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("followup_task")
    }

    fn spec(&self) -> ToolSpec {
        message_spec(
            "followup_task",
            "Send a plain-text follow-up to an existing ACP agent and start its next turn when idle.",
        )
    }

    fn handle(&self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'_> {
        Box::pin(async move {
            deliver(invocation, DeliveryMode::TriggerTurn)
                .await
                .map(boxed_tool_output)
        })
    }
}

impl CoreToolRuntime for FollowupHandler {
    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

async fn spawn(invocation: ToolInvocation) -> Result<FunctionToolOutput, FunctionCallError> {
    let ToolInvocation {
        session,
        step_context,
        payload,
        call_id,
        ..
    } = invocation;
    let turn = &step_context.turn;
    let args: SpawnArgs = parse_arguments(&function_arguments(payload)?)?;
    let message = message_content(args.message)?;
    let explicit_backend = explicit_backend(args.harness, args.model, args.effort)?;
    let mut config = build_agent_spawn_config(
        &session.get_base_instructions().await,
        turn.as_ref(),
        step_context.environments.primary(),
    )?;
    apply_spawn_agent_runtime_overrides(
        &mut config,
        turn.as_ref(),
        step_context.environments.primary(),
    )?;
    let role_name = args
        .agent_type
        .as_deref()
        .map(str::trim)
        .filter(|role_name| !role_name.is_empty());
    let role_settings = match role_name {
        Some(role_name) => acp_role_settings(&config, role_name)
            .await
            .map_err(FunctionCallError::RespondToModel)?,
        None => AcpRoleSettings {
            developer_instructions: None,
            backends: Vec::new(),
        },
    };
    let backend = resolve_backend(explicit_backend, &role_settings.backends)?;
    let harness = backend.harness.trim().to_string();
    if !is_valid_harness_id(&harness) {
        return Err(FunctionCallError::RespondToModel(
            "harness must contain 1-64 lowercase ASCII letters, digits, or hyphens".to_string(),
        ));
    }
    let message =
        with_role_developer_instructions(role_settings.developer_instructions.as_deref(), message);
    let spawn_source = thread_spawn_source(
        session.thread_id,
        &turn.session_source,
        next_thread_spawn_depth(&turn.session_source),
        Some(role_name.unwrap_or(ACP_ROLE_NAME)),
        Some(args.task_name),
    )?;
    let agent_path = spawn_source.get_agent_path().ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "spawned ACP agent is missing a canonical task name".to_string(),
        )
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
    let backend = acp_backend(harness, backend.model, backend.effort);
    let spawned = session
        .services
        .agent_control
        .spawn_external_agent_with_communication(
            config,
            backend,
            communication,
            AgentCommunicationContext::new(AgentCommunicationKind::Spawn, session.thread_id),
            spawn_source,
        )
        .await
        .map_err(collab_spawn_error)?;
    emit_sub_agent_activity(
        &session,
        turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: spawned.thread_id,
            agent_path: agent_path.clone(),
            kind: SubAgentActivityKind::Started,
        },
    )
    .await;
    Ok(FunctionToolOutput::from_text(
        serde_json::json!({"task_name": agent_path}).to_string(),
        Some(true),
    ))
}

fn explicit_backend(
    harness: Option<String>,
    model: Option<String>,
    effort: Option<String>,
) -> Result<AcpBackendOverrides, FunctionCallError> {
    let harness = harness.map(|value| value.trim().to_string());
    let model = model.map(|value| value.trim().to_string());
    let effort = effort.map(|value| value.trim().to_string());
    if harness.as_deref() == Some("") {
        return Err(FunctionCallError::RespondToModel(
            "harness must not be empty when specified".to_string(),
        ));
    }
    if model.as_deref() == Some("") {
        return Err(FunctionCallError::RespondToModel(
            "model must not be empty when specified".to_string(),
        ));
    }
    if effort.as_deref() == Some("") {
        return Err(FunctionCallError::RespondToModel(
            "effort must not be empty when specified".to_string(),
        ));
    }
    Ok(AcpBackendOverrides {
        harness,
        model,
        effort,
    })
}

fn resolve_backend(
    explicit: AcpBackendOverrides,
    role_backends: &[crate::agent::role::ExternalAgentBackend],
) -> Result<crate::agent::role::ExternalAgentBackend, FunctionCallError> {
    let selected = match explicit.harness.as_deref() {
        Some(harness) => role_backends
            .iter()
            .find(|candidate| {
                candidate.harness == harness
                    && explicit
                        .model
                        .as_ref()
                        .is_none_or(|model| candidate.model.as_ref() == Some(model))
            })
            .cloned()
            .unwrap_or_else(|| acp_backend(harness.to_string(), None, None)),
        None => role_backends
            .first()
            .cloned()
            .unwrap_or_else(|| acp_backend(String::new(), None, None)),
    };
    let harness = explicit.harness.unwrap_or(selected.harness);
    if harness.is_empty() {
        return Err(FunctionCallError::RespondToModel(
            "harness is required unless agent_type declares an ACP backend".to_string(),
        ));
    }
    Ok(acp_backend(
        harness,
        explicit.model.or(selected.model),
        explicit.effort.or(selected.effort),
    ))
}

fn with_role_developer_instructions(
    role_instructions: Option<&str>,
    task_message: String,
) -> String {
    match role_instructions {
        Some(role_instructions) => {
            format!("Role instructions:\n{role_instructions}\n\nTask:\n{task_message}")
        }
        None => task_message,
    }
}

#[cfg(test)]
#[path = "acp_agents_tests.rs"]
mod tests;

fn is_valid_harness_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

async fn deliver(
    invocation: ToolInvocation,
    mode: DeliveryMode,
) -> Result<FunctionToolOutput, FunctionCallError> {
    let ToolInvocation {
        session,
        step_context,
        payload,
        call_id,
        ..
    } = invocation;
    let turn = &step_context.turn;
    let args: MessageArgs = parse_arguments(&function_arguments(payload)?)?;
    let message = message_content(args.message)?;
    let agent_id = resolve_agent_target(&session, turn, &args.target).await?;
    if !session.services.agent_control.is_external_agent(agent_id) {
        return Err(FunctionCallError::RespondToModel(
            "ACP messaging tools require a target created by acp.spawn".to_string(),
        ));
    }
    let known = session
        .services
        .agent_control
        .ensure_agent_known(agent_id)
        .map_err(|error| collab_agent_error(agent_id, error))?;
    let agent_path = known.agent_path.ok_or_else(|| {
        FunctionCallError::RespondToModel("target agent is missing an agent_path".to_string())
    })?;
    let communication = InterAgentCommunication::new(
        turn.session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root),
        agent_path.clone(),
        Vec::new(),
        message,
        mode.trigger_turn(),
    );
    session
        .services
        .agent_control
        .send_inter_agent_communication(
            agent_id,
            communication,
            AgentCommunicationContext::new(mode.communication_kind(), session.thread_id),
            mode.trigger_turn().then(|| turn.sub_id.clone()),
            turn.turn_metadata_state.root_turn_id(),
        )
        .await
        .map_err(|error| collab_agent_error(agent_id, error))?;
    emit_sub_agent_activity(
        &session,
        turn,
        SubAgentActivityItem {
            id: call_id,
            agent_thread_id: agent_id,
            agent_path,
            kind: SubAgentActivityKind::Interacted,
        },
    )
    .await;
    Ok(FunctionToolOutput::from_text(String::new(), Some(true)))
}
