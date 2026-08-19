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

fn require_team_session_id(
    invocation: &ToolInvocation,
    explicit: Option<String>,
) -> Result<codex_team_runtime::TeamSessionId, FunctionCallError> {
    if let Some(value) = explicit {
        return codex_team_runtime::TeamSessionId::parse(value)
            .map_err(FunctionCallError::RespondToModel);
    }
    if let Some(binding) = invocation
        .session
        .services
        .agent_control
        .team()
        .binding_snapshot(&invocation.session.thread_id.to_string())
    {
        return Ok(binding.team_session_id);
    }
    Err(FunctionCallError::RespondToModel(
        "root coordinator tools require team_session_id".to_string(),
    ))
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
