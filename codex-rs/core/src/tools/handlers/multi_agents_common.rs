use crate::agent::role::apply_role_to_config;
use crate::agent::role::apply_role_to_config_for_multi_agent_v2;
use crate::config::Config;
use crate::config::DEFAULT_MULTI_AGENT_V2_MIN_WAIT_TIMEOUT_MS;
use crate::config::HARD_MAX_MULTI_AGENT_V2_TIMEOUT_MS;
use crate::function_tool::FunctionCallError;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;
use crate::session::turn_context::TurnEnvironment;
use crate::tools::context::FunctionToolOutput;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use codex_models_manager::manager::RefreshStrategy;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::openai_models::ModelPreset;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::openai_models::ReasoningEffortPreset;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::user_input::UserInput;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

/// Minimum wait timeout to prevent tight polling loops from burning CPU.
pub(crate) const MIN_WAIT_TIMEOUT_MS: i64 = DEFAULT_MULTI_AGENT_V2_MIN_WAIT_TIMEOUT_MS;
pub(crate) const DEFAULT_WAIT_TIMEOUT_MS: i64 = 30_000;
pub(crate) const MAX_WAIT_TIMEOUT_MS: i64 = HARD_MAX_MULTI_AGENT_V2_TIMEOUT_MS;
pub(crate) const MAX_SPAWN_AGENT_MODEL_OVERRIDES: usize = 5;
pub(crate) const MAX_SPAWN_METADATA_ENTRIES: usize = 16;
pub(crate) const MAX_SPAWN_METADATA_KEY_CHARS: usize = 64;
pub(crate) const MAX_SPAWN_METADATA_VALUE_CHARS: usize = 512;

/// 起動時metadataをobserver向けラベルとして受理する。secretやprompt注入には使わない。
pub(crate) fn parse_spawn_observer_metadata(
    raw: Option<JsonValue>,
) -> Result<Option<BTreeMap<String, String>>, FunctionCallError> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let JsonValue::Object(map) = value else {
        return Err(FunctionCallError::RespondToModel(
            "metadata must be an object of string keys and string values".to_string(),
        ));
    };
    normalize_spawn_observer_metadata(map)
}

fn normalize_spawn_observer_metadata(
    map: Map<String, JsonValue>,
) -> Result<Option<BTreeMap<String, String>>, FunctionCallError> {
    if map.is_empty() {
        return Ok(None);
    }
    if map.len() > MAX_SPAWN_METADATA_ENTRIES {
        return Err(FunctionCallError::RespondToModel(format!(
            "metadata supports at most {MAX_SPAWN_METADATA_ENTRIES} entries"
        )));
    }
    let mut metadata = BTreeMap::new();
    for (key, value) in map {
        if key.is_empty() || key.chars().count() > MAX_SPAWN_METADATA_KEY_CHARS {
            return Err(FunctionCallError::RespondToModel(format!(
                "metadata keys must be 1-{MAX_SPAWN_METADATA_KEY_CHARS} characters"
            )));
        }
        let JsonValue::String(text) = value else {
            return Err(FunctionCallError::RespondToModel(
                "metadata values must be strings".to_string(),
            ));
        };
        if text.chars().count() > MAX_SPAWN_METADATA_VALUE_CHARS {
            return Err(FunctionCallError::RespondToModel(format!(
                "metadata values must be at most {MAX_SPAWN_METADATA_VALUE_CHARS} characters"
            )));
        }
        metadata.insert(key, text);
    }
    Ok(Some(metadata))
}

pub(crate) fn model_supports_multi_agent_backend(
    model: &ModelPreset,
    multi_agent_version: MultiAgentVersion,
) -> bool {
    multi_agent_version != MultiAgentVersion::V2
        || model.multi_agent_version != Some(MultiAgentVersion::Disabled)
}

pub(crate) fn function_arguments(payload: ToolPayload) -> Result<String, FunctionCallError> {
    match payload {
        ToolPayload::Function { arguments } => Ok(arguments),
        _ => Err(FunctionCallError::RespondToModel(
            "collab handler received unsupported payload".to_string(),
        )),
    }
}

pub(crate) fn tool_output_json_text<T>(value: &T, tool_name: &str) -> String
where
    T: Serialize,
{
    serde_json::to_string(value).unwrap_or_else(|err| {
        JsonValue::String(format!("failed to serialize {tool_name} result: {err}")).to_string()
    })
}

pub(crate) fn tool_output_response_item<T>(
    call_id: &str,
    payload: &ToolPayload,
    value: &T,
    success: Option<bool>,
    tool_name: &str,
) -> ResponseInputItem
where
    T: Serialize,
{
    FunctionToolOutput::from_text(tool_output_json_text(value, tool_name), success)
        .to_response_item(call_id, payload)
}

pub(crate) fn tool_output_code_mode_result<T>(value: &T, tool_name: &str) -> JsonValue
where
    T: Serialize,
{
    serde_json::to_value(value).unwrap_or_else(|err| {
        JsonValue::String(format!("failed to serialize {tool_name} result: {err}"))
    })
}

pub(crate) fn collab_spawn_error(err: CodexErr) -> FunctionCallError {
    match err.details() {
        CodexErrorDetails::UnsupportedOperation(message) if message == "thread manager dropped" => {
            FunctionCallError::RespondToModel("collab manager unavailable".to_string())
        }
        CodexErrorDetails::UnsupportedOperation(message) => {
            FunctionCallError::RespondToModel(message.clone())
        }
        _ => FunctionCallError::RespondToModel(format!("collab spawn failed: {err}")),
    }
}

pub(crate) fn collab_agent_error(agent_id: ThreadId, err: CodexErr) -> FunctionCallError {
    match err.details() {
        CodexErrorDetails::ThreadNotFound(id) => {
            FunctionCallError::RespondToModel(format!("agent with id {id} not found"))
        }
        CodexErrorDetails::InternalAgentDied => {
            FunctionCallError::RespondToModel(format!("agent with id {agent_id} is closed"))
        }
        CodexErrorDetails::UnsupportedOperation(_) => {
            FunctionCallError::RespondToModel("collab manager unavailable".to_string())
        }
        _ => FunctionCallError::RespondToModel(format!("collab tool failed: {err}")),
    }
}

#[derive(Clone, Copy)]
pub(crate) enum RawCollaborationOp {
    Spawn,
    SendMessage,
    FollowupTask,
    Wait,
    Interrupt,
    Close,
    Resume,
}

impl RawCollaborationOp {
    fn team_tool(self) -> &'static str {
        match self {
            Self::Spawn | Self::Resume => "team.spawn_agent",
            Self::SendMessage => "team.send_message",
            Self::FollowupTask => "team.followup_agent",
            Self::Wait => "team.wait",
            Self::Interrupt | Self::Close => "team.interrupt_agent",
        }
    }

    fn raw_tool(self) -> &'static str {
        match self {
            Self::Spawn => "collaboration.spawn_agent",
            Self::SendMessage => "collaboration.send_message",
            Self::FollowupTask => "collaboration.followup_task",
            Self::Wait => "collaboration.wait_agent",
            Self::Interrupt => "collaboration.interrupt_agent",
            Self::Close => "multi_agent_v1.close_agent",
            Self::Resume => "multi_agent_v1.resume_agent",
        }
    }

    fn unsupported_as_raw_team_op(self) -> bool {
        matches!(self, Self::Close | Self::Resume)
    }
}

/// caller またはいずれかの target が Team-bound なら raw collaboration を拒否する。
pub(crate) fn reject_team_bound_raw_collaboration(
    session: &Session,
    caller_thread_id: &str,
    target_thread_ids: &[&str],
    op: RawCollaborationOp,
) -> Result<(), FunctionCallError> {
    let team = session.services.agent_control.team();
    let caller_binding = team.binding_snapshot(caller_thread_id);
    let target_binding = target_thread_ids
        .iter()
        .copied()
        .find_map(|target| team.binding_snapshot(target));
    let Some(binding) = caller_binding.as_ref().or(target_binding.as_ref()) else {
        return Ok(());
    };
    let message = if op.unsupported_as_raw_team_op() {
        format!(
            "Team-bound {} is unsupported. Use {}(team_session_id={}, ...) as the managed Team operation. {} cannot be used when the caller or any target is bound to a Team.",
            op.raw_tool(),
            op.team_tool(),
            binding.team_session_id,
            op.raw_tool(),
        )
    } else {
        format!(
            "Team-bound collaboration must use {}(team_session_id={}, ...). {} cannot be used when the caller or any target is bound to a Team.",
            op.team_tool(),
            binding.team_session_id,
            op.raw_tool(),
        )
    };
    Err(FunctionCallError::RespondToModel(message))
}

/// 未所属 root が open Team ありのまま raw spawn すると帰属を推測できない。
pub(crate) fn reject_unbound_raw_spawn_when_teams_open(
    session: &Session,
    caller_thread_id: &str,
    spawn_tool: &str,
) -> Result<(), FunctionCallError> {
    let team = session.services.agent_control.team();
    if team.binding_snapshot(caller_thread_id).is_none() && team.open_team_count() > 0 {
        return Err(FunctionCallError::RespondToModel(format!(
            "open Team sessions require team.spawn_agent(team_session_id, ...). {spawn_tool} cannot infer Team identity."
        )));
    }
    Ok(())
}

pub(crate) fn thread_spawn_source(
    parent_thread_id: ThreadId,
    parent_session_source: &SessionSource,
    depth: i32,
    agent_role: Option<&str>,
    task_name: Option<String>,
) -> Result<SessionSource, FunctionCallError> {
    let agent_path = task_name
        .as_deref()
        .map(|task_name| {
            parent_session_source
                .get_agent_path()
                .unwrap_or_else(AgentPath::root)
                .join(task_name)
                .map_err(FunctionCallError::RespondToModel)
        })
        .transpose()?;
    Ok(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
        parent_thread_id,
        depth,
        agent_path,
        agent_nickname: None,
        agent_role: agent_role.map(str::to_string),
    }))
}

pub(crate) fn parse_collab_input(
    message: Option<String>,
    items: Option<Vec<UserInput>>,
) -> Result<Vec<UserInput>, FunctionCallError> {
    match (message, items) {
        (Some(_), Some(_)) => Err(FunctionCallError::RespondToModel(
            "Provide either message or items, but not both".to_string(),
        )),
        (None, None) => Err(FunctionCallError::RespondToModel(
            "Provide one of: message or items".to_string(),
        )),
        (Some(message), None) => {
            if message.trim().is_empty() {
                return Err(FunctionCallError::RespondToModel(
                    "Empty message can't be sent to an agent".to_string(),
                ));
            }
            Ok(vec![UserInput::Text {
                text: message,
                text_elements: Vec::new(),
            }])
        }
        (None, Some(items)) => {
            if items.is_empty() {
                return Err(FunctionCallError::RespondToModel(
                    "Items can't be empty".to_string(),
                ));
            }
            Ok(items)
        }
    }
}

/// Builds the base config snapshot for a newly spawned sub-agent.
///
/// The returned config starts from the parent's effective config and then refreshes the
/// runtime-owned fields carried by the turn and selected environment, including model selection,
/// reasoning settings, approval policy, sandbox, and cwd. Role-specific overrides are layered
/// after this step; skipping this helper and cloning stale config state directly can send the child
/// agent out with the wrong provider or runtime policy.
pub(crate) fn build_agent_spawn_config(
    base_instructions: &BaseInstructions,
    turn: &TurnContext,
    environment: Option<&TurnEnvironment>,
) -> Result<Config, FunctionCallError> {
    let mut config = build_agent_shared_config(turn, environment)?;
    config.base_instructions = Some(base_instructions.text.clone());
    config.base_instructions_provenance = base_instructions.provenance.clone();
    Ok(config)
}

pub(crate) fn build_agent_resume_config(
    turn: &TurnContext,
    environment: Option<&TurnEnvironment>,
) -> Result<Config, FunctionCallError> {
    let mut config = build_agent_shared_config(turn, environment)?;
    // For resume, keep base instructions sourced from rollout/session metadata.
    config.base_instructions = None;
    config.base_instructions_provenance = None;
    Ok(config)
}

fn build_agent_shared_config(
    turn: &TurnContext,
    environment: Option<&TurnEnvironment>,
) -> Result<Config, FunctionCallError> {
    let base_config = turn.config.clone();
    let mut config = (*base_config).clone();
    config.model = Some(turn.model_info.slug.clone());
    config.model_provider = turn.provider.info().clone();
    config.model_reasoning_effort = turn
        .reasoning_effort
        .clone()
        .or_else(|| turn.model_info.default_reasoning_level.clone());
    config.model_reasoning_summary = Some(turn.reasoning_summary);
    config.developer_instructions = turn.developer_instructions.clone();
    if turn.multi_agent_version == MultiAgentVersion::V2
        && let Some(developer_instructions) = turn
            .config
            .multi_agent_v2
            .subagent_developer_instructions
            .clone()
    {
        config.developer_instructions = Some(developer_instructions);
    }
    apply_spawn_agent_runtime_overrides(&mut config, turn, environment)?;

    Ok(config)
}

pub(crate) fn reject_full_fork_agent_type_override(
    agent_type: Option<&str>,
) -> Result<(), FunctionCallError> {
    if agent_type.is_some() {
        return Err(FunctionCallError::RespondToModel(
            "Full-history forked agents inherit the parent agent type; omit agent_type, or spawn without a full-history fork.".to_string(),
        ));
    }
    Ok(())
}

/// Copies runtime-only turn state onto a child config before it is handed to `AgentControl`.
///
/// These values are chosen by the live turn and selected environment rather than persisted config,
/// so leaving them stale can make a child agent disagree with its parent about approval policy,
/// cwd, or sandboxing.
pub(crate) fn apply_spawn_agent_runtime_overrides(
    config: &mut Config,
    turn: &TurnContext,
    environment: Option<&TurnEnvironment>,
) -> Result<(), FunctionCallError> {
    config
        .permissions
        .approval_policy
        .set(turn.approval_policy())
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("approval_policy is invalid: {err}"))
        })?;
    config.approvals_reviewer = turn.config.approvals_reviewer;
    #[allow(deprecated)]
    let turn_cwd = turn.cwd.clone();
    config.cwd = turn_cwd;
    let permission_profile = environment
        .map(|environment| environment.permission_profile().clone())
        .unwrap_or_else(|| turn.permission_profile());
    config
        .permissions
        .set_permission_profile(permission_profile)
        .map_err(|err| {
            FunctionCallError::RespondToModel(format!("permission_profile is invalid: {err}"))
        })?;
    Ok(())
}

pub(crate) async fn apply_requested_spawn_agent_model_overrides(
    session: &Session,
    turn: &TurnContext,
    config: &mut Config,
    requested_model: Option<&str>,
    requested_reasoning_effort: Option<ReasoningEffort>,
) -> Result<(), FunctionCallError> {
    let requested_model = requested_model.or(turn.config.agent_default_subagent_model.as_deref());
    let requested_reasoning_effort = requested_reasoning_effort
        .or_else(|| turn.config.agent_default_subagent_reasoning_effort.clone());
    if requested_model.is_none() && requested_reasoning_effort.is_none() {
        return Ok(());
    }

    if let Some(requested_model) = requested_model {
        let available_models = session
            .services
            .models_manager
            .list_models(RefreshStrategy::Offline, config.http_client_factory())
            .await;
        let selected_model_name = find_spawn_agent_model_name(
            &available_models,
            requested_model,
            turn.multi_agent_version,
        )?;
        let selected_model_info = session
            .services
            .models_manager
            .get_model_info(&selected_model_name, &config.to_models_manager_config())
            .await;

        config.model = Some(selected_model_name.clone());
        if let Some(reasoning_effort) = requested_reasoning_effort {
            validate_spawn_agent_reasoning_effort(
                &selected_model_name,
                &selected_model_info.supported_reasoning_levels,
                &reasoning_effort,
            )?;
            config.model_reasoning_effort = Some(reasoning_effort);
        } else {
            config.model_reasoning_effort = selected_model_info.default_reasoning_level;
        }

        return Ok(());
    }

    if let Some(reasoning_effort) = requested_reasoning_effort {
        validate_spawn_agent_reasoning_effort(
            &turn.model_info.slug,
            &turn.model_info.supported_reasoning_levels,
            &reasoning_effort,
        )?;
        config.model_reasoning_effort = Some(reasoning_effort);
    }

    Ok(())
}

pub(crate) async fn apply_spawn_agent_service_tier(
    session: &Session,
    config: &mut Config,
    parent_service_tier: Option<&str>,
    requested_service_tier: Option<&str>,
) -> Result<(), FunctionCallError> {
    let candidate_service_tiers = [
        config.service_tier.clone(),
        requested_service_tier.map(str::to_string),
        parent_service_tier.map(str::to_string),
    ];
    if candidate_service_tiers.iter().all(Option::is_none) {
        config.service_tier = None;
        return Ok(());
    }

    let model = config.model.clone().ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "spawn_agent could not resolve the child model for service tier validation".to_string(),
        )
    })?;
    let model_info = session
        .services
        .models_manager
        .get_model_info(model.as_str(), &config.to_models_manager_config())
        .await;

    if let Some(requested_service_tier) = requested_service_tier
        && !model_info.supports_service_tier(requested_service_tier)
    {
        let supported_service_tiers = if model_info.service_tiers.is_empty() {
            "none".to_string()
        } else {
            model_info
                .service_tiers
                .iter()
                .map(|tier| tier.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        return Err(FunctionCallError::RespondToModel(format!(
            "Service tier `{requested_service_tier}` is not supported for model `{model}`. Supported service tiers: {supported_service_tiers}"
        )));
    }

    config.service_tier =
        candidate_service_tiers
            .into_iter()
            .flatten()
            .find(|candidate_service_tier| {
                model_info.supports_service_tier(candidate_service_tier.as_str())
            });
    Ok(())
}

pub(crate) async fn apply_spawn_agent_role(
    session: &Session,
    config: &mut Config,
    role_name: Option<&str>,
) -> Result<(), FunctionCallError> {
    let previous_model = config.model.clone();
    let previous_reasoning_effort = config.model_reasoning_effort.clone();
    if session.multi_agent_version() == Some(MultiAgentVersion::V2) {
        apply_role_to_config_for_multi_agent_v2(config, role_name)
            .await
            .map_err(FunctionCallError::RespondToModel)?;
    } else {
        apply_role_to_config(config, role_name)
            .await
            .map_err(FunctionCallError::RespondToModel)?;
    }
    if config.model == previous_model && config.model_reasoning_effort == previous_reasoning_effort
    {
        return Ok(());
    }

    let Some(reasoning_effort) = config.model_reasoning_effort.clone() else {
        return Ok(());
    };
    let model = config.model.clone().ok_or_else(|| {
        FunctionCallError::RespondToModel(
            "spawn_agent could not resolve the child model for reasoning effort validation"
                .to_string(),
        )
    })?;
    let model_info = session
        .services
        .models_manager
        .get_model_info(&model, &config.to_models_manager_config())
        .await;
    if model_info.used_fallback_model_metadata {
        return Ok(());
    }

    validate_spawn_agent_reasoning_effort(
        &model,
        &model_info.supported_reasoning_levels,
        &reasoning_effort,
    )
}

fn find_spawn_agent_model_name(
    available_models: &[ModelPreset],
    requested_model: &str,
    multi_agent_version: MultiAgentVersion,
) -> Result<String, FunctionCallError> {
    available_models
        .iter()
        .find(|model| {
            model.model == requested_model
                && model_supports_multi_agent_backend(model, multi_agent_version)
        })
        .map(|model| model.model.clone())
        .ok_or_else(|| {
            let available = available_models
                .iter()
                .filter(|model| model.show_in_picker)
                .filter(|model| model_supports_multi_agent_backend(model, multi_agent_version))
                .take(MAX_SPAWN_AGENT_MODEL_OVERRIDES)
                .map(|model| model.model.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            FunctionCallError::RespondToModel(format!(
                "Unknown model `{requested_model}` for spawn_agent. Available models: {available}"
            ))
        })
}

fn validate_spawn_agent_reasoning_effort(
    model: &str,
    supported_reasoning_levels: &[ReasoningEffortPreset],
    requested_reasoning_effort: &ReasoningEffort,
) -> Result<(), FunctionCallError> {
    if supported_reasoning_levels
        .iter()
        .any(|preset| &preset.effort == requested_reasoning_effort)
    {
        return Ok(());
    }

    let supported = supported_reasoning_levels
        .iter()
        .map(|preset| preset.effort.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    Err(FunctionCallError::RespondToModel(format!(
        "Reasoning effort `{requested_reasoning_effort}` is not supported for model `{model}`. Supported reasoning efforts: {supported}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use serde_json::json;

    #[test]
    fn omitted_or_empty_metadata_is_absent() {
        assert_eq!(parse_spawn_observer_metadata(None).expect("omit"), None);
        assert_eq!(
            parse_spawn_observer_metadata(Some(json!({}))).expect("empty"),
            None
        );
    }

    #[test]
    fn string_map_is_accepted() {
        let parsed = parse_spawn_observer_metadata(Some(json!({
            "issue": "3360",
            "note": "",
        })))
        .expect("valid metadata");
        assert_eq!(
            parsed,
            Some(BTreeMap::from([
                ("issue".to_string(), "3360".to_string()),
                ("note".to_string(), String::new()),
            ]))
        );
    }

    #[test]
    fn non_object_or_non_string_metadata_is_rejected() {
        assert!(parse_spawn_observer_metadata(Some(json!("x"))).is_err());
        assert!(parse_spawn_observer_metadata(Some(json!(["a"]))).is_err());
        assert!(parse_spawn_observer_metadata(Some(json!({"a": 1}))).is_err());
    }

    #[test]
    fn metadata_limits_are_rejected() {
        assert!(parse_spawn_observer_metadata(Some(json!({"": "x"}))).is_err());
        assert!(
            parse_spawn_observer_metadata(Some(json!({
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": "x"
            })))
            .is_err()
        );
        assert!(
            parse_spawn_observer_metadata(Some(json!({
                "a": "x".repeat(MAX_SPAWN_METADATA_VALUE_CHARS + 1)
            })))
            .is_err()
        );
        let too_many = (0..=MAX_SPAWN_METADATA_ENTRIES)
            .map(|index| (format!("k{index}"), json!("v")))
            .collect::<serde_json::Map<_, _>>();
        assert!(parse_spawn_observer_metadata(Some(JsonValue::Object(too_many))).is_err());
    }

    #[test]
    fn metadata_limits_count_unicode_scalars() {
        let scalar = "😀";
        assert_eq!(scalar.chars().count(), 1);
        let key_ok = scalar.repeat(MAX_SPAWN_METADATA_KEY_CHARS);
        let value_ok = scalar.repeat(MAX_SPAWN_METADATA_VALUE_CHARS);
        let parsed = parse_spawn_observer_metadata(Some(json!({ key_ok: value_ok })))
            .expect("scalar-length boundary must be accepted");
        assert_eq!(
            parsed.as_ref().map(std::collections::BTreeMap::len),
            Some(1)
        );

        assert!(
            parse_spawn_observer_metadata(Some(
                json!({ scalar.repeat(MAX_SPAWN_METADATA_KEY_CHARS + 1): "x" })
            ))
            .is_err()
        );
        assert!(
            parse_spawn_observer_metadata(Some(
                json!({ "a": scalar.repeat(MAX_SPAWN_METADATA_VALUE_CHARS + 1) })
            ))
            .is_err()
        );
    }
}
