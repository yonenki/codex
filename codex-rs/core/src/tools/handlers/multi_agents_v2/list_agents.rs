use super::analytics::ToolCallAnalytics;
use super::*;
use crate::tools::handlers::multi_agents_spec::create_list_agents_tool;
use codex_tools::ToolSpec;

pub(crate) struct Handler;

impl ToolExecutor<ToolInvocation> for Handler {
    fn tool_name(&self) -> ToolName {
        ToolName::plain("list_agents")
    }

    fn spec(&self) -> ToolSpec {
        create_list_agents_tool()
    }

    fn handle<'a>(&'a self, invocation: ToolInvocation) -> codex_tools::ToolExecutorFuture<'a>
    where
        ToolInvocation: 'a,
    {
        Box::pin(async move {
            let analytics = ToolCallAnalytics::new(&invocation, CollabAgentTool::ListAgents);
            let result = self.handle_call(invocation).await;
            analytics.finish(&result);
            result
        })
    }
}

impl Handler {
    async fn handle_call(
        &self,
        invocation: ToolInvocation,
    ) -> Result<Box<dyn crate::tools::context::ToolOutput>, FunctionCallError> {
        let ToolInvocation {
            session,
            turn,
            payload,
            ..
        } = invocation;
        let arguments = function_arguments(payload)?;
        let args: ListAgentsArgs = parse_arguments(&arguments)?;
        session
            .services
            .agent_control
            .register_session_root(session.thread_id, turn.parent_thread_id);
        let agents = session
            .services
            .agent_control
            .list_agents(&turn.session_source, args.path_prefix.as_deref())
            .await
            .map_err(collab_spawn_error)?;

        let total = agents.len();
        let limit = args.limit.unwrap_or(20).clamp(1, 100);
        let entries: Vec<_> = agents
            .into_iter()
            .skip(args.offset)
            .take(limit)
            .map(|agent| {
                let agent_status = match args.detail {
                    Detail::Full => ListedStatus::Full(agent.agent_status),
                    Detail::Summary => ListedStatus::Summary(match agent.agent_status {
                        AgentStatus::PendingInit => Lifecycle::PendingInit,
                        AgentStatus::Running => Lifecycle::Running,
                        AgentStatus::Interrupted => Lifecycle::Interrupted,
                        AgentStatus::Completed(_) => Lifecycle::Completed,
                        AgentStatus::Errored(_) => Lifecycle::Errored,
                        AgentStatus::Shutdown => Lifecycle::Shutdown,
                        AgentStatus::NotFound => Lifecycle::NotFound,
                    }),
                };
                AgentEntry {
                    agent_name: agent.agent_name,
                    agent_status,
                }
            })
            .collect();
        let end = args.offset.saturating_add(entries.len());
        Ok(boxed_tool_output(ListAgentsResult {
            agents: entries,
            total,
            next_offset: (end < total).then_some(end),
        }))
    }
}

impl CoreToolRuntime for Handler {
    fn team_lifecycle_routing(&self) -> TeamLifecycleRouting {
        TeamLifecycleRouting::HandlerOwned
    }

    fn matches_kind(&self, payload: &ToolPayload) -> bool {
        matches!(payload, ToolPayload::Function { .. })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListAgentsArgs {
    path_prefix: Option<String>,
    #[serde(default)]
    detail: Detail,
    #[serde(default)]
    offset: usize,
    limit: Option<usize>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Detail {
    #[default]
    Summary,
    Full,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum Lifecycle {
    PendingInit,
    Running,
    Interrupted,
    Completed,
    Errored,
    Shutdown,
    NotFound,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ListedStatus {
    Summary(Lifecycle),
    Full(AgentStatus),
}

#[derive(Debug, Serialize)]
struct AgentEntry {
    agent_name: String,
    agent_status: ListedStatus,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListAgentsResult {
    agents: Vec<AgentEntry>,
    total: usize,
    next_offset: Option<usize>,
}

impl ToolOutput for ListAgentsResult {
    fn log_output(&self) -> String {
        tool_output_json_text(self, "list_agents")
    }

    fn success_for_logging(&self) -> bool {
        true
    }

    fn to_response_item(&self, call_id: &str, payload: &ToolPayload) -> ResponseInputItem {
        tool_output_response_item(call_id, payload, self, Some(true), "list_agents")
    }

    fn code_mode_result(&self, _payload: &ToolPayload) -> JsonValue {
        tool_output_code_mode_result(self, "list_agents")
    }
}
