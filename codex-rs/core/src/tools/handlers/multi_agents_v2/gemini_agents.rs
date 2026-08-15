use super::*;
use crate::agent::next_thread_spawn_depth;
use crate::agent::role::ANTIGRAVITY_ROLE_NAME;
use crate::agent::role::antigravity_backend;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::tools::context::FunctionToolOutput;
use crate::tools::handlers::multi_agents_v2::message_tool::message_content;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;
use std::collections::BTreeMap;

pub(crate) struct SpawnHandler;
pub(crate) struct MessageHandler;
pub(crate) struct FollowupHandler;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SpawnArgs {
    task_name: String,
    message: String,
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
                "Task name for the Gemini agent. Use lowercase letters, digits, and underscores."
                    .to_string(),
            )),
        ),
        (
            "message".to_string(),
            JsonSchema::string(Some(
                "Complete plain-text task for Gemini. This text is passed to the local Antigravity CLI and no parent history is inherited."
                    .to_string(),
            )),
        ),
    ]);
    ToolSpec::Function(ResponsesApiTool {
        name: "spawn".to_string(),
        description: concat!(
            "Spawn a Gemini 3.7 Flash subagent owned by the Codex agent-control plane. ",
            "The local Antigravity process shares the current working directory and runs ",
            "non-interactively with permission checks bypassed, so it can modify the workspace ",
            "outside Codex sandbox enforcement. Use only when that external execution is ",
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

fn message_spec(name: &str, description: &str) -> ToolSpec {
    let properties = BTreeMap::from([
        (
            "target".to_string(),
            JsonSchema::string(Some(
                "Canonical or relative task name returned by gemini.spawn.".to_string(),
            )),
        ),
        (
            "message".to_string(),
            JsonSchema::string(Some(
                "Plain-text message passed to the local Antigravity CLI.".to_string(),
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
            "Queue a plain-text message for an existing Gemini agent without starting a turn.",
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
            "Send a plain-text follow-up to an existing Gemini agent and start its next turn when idle.",
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
    let spawn_source = thread_spawn_source(
        session.thread_id,
        &turn.session_source,
        next_thread_spawn_depth(&turn.session_source),
        Some(ANTIGRAVITY_ROLE_NAME),
        Some(args.task_name),
    )?;
    let agent_path = spawn_source.get_agent_path().ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "spawned Gemini agent is missing a canonical task name".to_string(),
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
    let backend = antigravity_backend();
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
            "Gemini messaging tools require a target created by gemini.spawn".to_string(),
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
