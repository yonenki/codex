use crate::TurnInputRequest;
use crate::TurnInputSubmission;
use crate::TurnStartOptions;
use crate::agent::AgentStatus;
use crate::agent::registry::AgentMetadata;
use crate::agent::registry::AgentRegistry;
use crate::agent::role::DEFAULT_ROLE_NAME;
use crate::agent::role::resolve_role_config;
use crate::agent::status::is_final;
use crate::agent_communication::AgentCommunicationContext;
use crate::agent_communication::AgentCommunicationKind;
use crate::codex_thread::ThreadConfigSnapshot;
use crate::config::Config;
use crate::config::RolloutBudgetConfig;
use crate::environment_selection::TurnEnvironmentSnapshot;
use crate::rollout_budget::RolloutBudget;
use crate::session::emit_subagent_session_started;
use crate::session::multi_agents::ResolvedMultiAgentV2UsageHints;
use crate::session_prefix::format_inter_agent_completion_message;
use crate::session_prefix::format_subagent_context_line;
use crate::session_prefix::format_subagent_notification_message;
use crate::thread_manager::ResumeThreadWithHistoryOptions;
use crate::thread_manager::ThreadIdGenerator;
use crate::thread_manager::ThreadManagerState;
use crate::thread_manager::default_thread_id_generator;
use crate::thread_rollout_truncation::truncate_rollout_to_last_n_fork_turns;
use codex_history::InitialHistory;
use codex_history::ResumedHistory;
use codex_history::RolloutItem;
use codex_protocol::AgentPath;
use codex_protocol::SessionId;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::CodexErrorDetails;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::models::ContentItem;
use codex_protocol::models::MessagePhase;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::InterAgentCommunication;
use codex_protocol::protocol::MultiAgentVersion;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::SubAgentSource;
use codex_protocol::protocol::ThreadHistoryMode;
use codex_protocol::protocol::ThreadSource;
use codex_protocol::protocol::TurnEnvironmentSelection;
use codex_protocol::user_input::UserInput;
use codex_thread_store::LoadThreadHistoryParams;
use codex_thread_store::ReadThreadParams;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use tokio::sync::watch;
use tracing::warn;
use uuid::Uuid;

pub(crate) use self::execution::AgentExecutionGuard;
use self::execution::AgentExecutionLimiter;
use self::residency::V2Residency;

mod execution;
mod external;
mod legacy;
mod residency;
mod spawn;
mod terminal_notification;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SpawnAgentForkMode {
    FullHistory,
    LastNTurns(usize),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpawnAgentOptions {
    pub(crate) fork_parent_spawn_call_id: Option<String>,
    pub(crate) fork_mode: Option<SpawnAgentForkMode>,
    pub(crate) parent_thread_id: Option<ThreadId>,
    pub(crate) parent_turn_id: Option<String>,
    pub(crate) root_turn_id: Option<String>,
    pub(crate) environments: Option<Vec<TurnEnvironmentSelection>>,
    pub(crate) multi_agent_v2_usage_hints: Option<ResolvedMultiAgentV2UsageHints>,
    pub(crate) metadata: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Clone, Debug)]
pub(crate) struct LiveAgent {
    pub(crate) thread_id: ThreadId,
    pub(crate) metadata: AgentMetadata,
    pub(crate) status: AgentStatus,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ListedAgent {
    pub(crate) agent_name: String,
    pub(crate) agent_status: AgentStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExternalBackendRoute {
    role_name: String,
    candidate_index: usize,
    fallback_claimed: bool,
}

/// Control-plane handle for multi-agent operations.
/// `AgentControl` is held by each session (via `SessionServices`). It provides capability to
/// spawn new agents and the inter-agent communication layer.
/// An `AgentControl` instance is intended to be created at most once per root thread/session
/// tree. That same `AgentControl` is then shared with every sub-agent spawned from that root,
/// which keeps the registry scoped to that root thread rather than the entire `ThreadManager`.
#[derive(Clone)]
pub(crate) struct AgentControl {
    /// ID shared by the whole agent control session. This means every sub-agents from a common
    /// root share the same session ID.
    session_id: SessionId,
    /// Weak handle back to the global thread registry/state.
    /// This is `Weak` to avoid reference cycles and shadow persistence of the form
    /// `ThreadManagerState -> CodexThread -> Session -> SessionServices -> ThreadManagerState`.
    manager: Weak<ThreadManagerState>,
    /// Captured at construction so delegates retain their manager's allocation policy.
    thread_id_generator: ThreadIdGenerator,
    state: Arc<AgentRegistry>,
    external_agents: Arc<external::ExternalAgentManager>,
    external_backend_routes: Arc<Mutex<HashMap<ThreadId, ExternalBackendRoute>>>,
    external_completion_watchers: Arc<Mutex<HashSet<ThreadId>>>,
    v2_residency: Arc<V2Residency>,
    agent_execution_limiter: Arc<AgentExecutionLimiter>,
    /// Session-scoped state shared by the root thread and every cloned sub-agent control handle.
    rollout_budget: Arc<RolloutBudget>,
}

impl Default for AgentControl {
    fn default() -> Self {
        Self::new(
            Weak::default(),
            default_thread_id_generator(),
            /*rollout_budget*/ None,
        )
    }
}

impl AgentControl {
    pub(crate) fn is_external_agent(&self, agent_id: ThreadId) -> bool {
        self.external_agents.contains(agent_id)
    }

    pub(crate) fn external_backend_identity(
        &self,
        agent_id: ThreadId,
    ) -> Option<(String, Option<String>)> {
        self.external_agents
            .identity(agent_id)
            .map(|identity| (identity.harness, identity.model))
    }

    /// Construct a new `AgentControl` that can spawn/message agents via the given manager state.
    pub(crate) fn new(
        manager: Weak<ThreadManagerState>,
        thread_id_generator: ThreadIdGenerator,
        rollout_budget: Option<RolloutBudgetConfig>,
    ) -> Self {
        let control = Self {
            session_id: SessionId::default(),
            manager,
            thread_id_generator,
            state: Arc::default(),
            external_agents: Arc::default(),
            external_backend_routes: Arc::default(),
            external_completion_watchers: Arc::default(),
            v2_residency: Arc::default(),
            agent_execution_limiter: Arc::default(),
            rollout_budget: Arc::default(),
        };
        if let Some(rollout_budget) = rollout_budget {
            control.rollout_budget.configure(rollout_budget);
        }
        control
    }

    pub(crate) fn with_session_id(mut self, session_id: SessionId, max_threads: usize) -> Self {
        self.session_id = session_id;
        self.agent_execution_limiter.initialize(max_threads);
        self
    }

    pub(crate) fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub(crate) fn generate_thread_id(&self) -> ThreadId {
        (self.thread_id_generator)()
    }

    pub(crate) fn record_external_backend_route(
        &self,
        agent_id: ThreadId,
        role_name: String,
        candidate_index: usize,
    ) {
        self.external_backend_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                agent_id,
                ExternalBackendRoute {
                    role_name,
                    candidate_index,
                    fallback_claimed: false,
                },
            );
    }

    pub(crate) fn claim_next_external_backend_candidate(
        &self,
        agent_id: ThreadId,
        role_name: &str,
    ) -> CodexResult<usize> {
        let mut routes = self
            .external_backend_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let route = routes.get_mut(&agent_id).ok_or_else(|| {
            CodexErr::UnsupportedOperation(
                "fallback source was not spawned from an ACP backend pool".to_string(),
            )
        })?;
        if route.role_name != role_name {
            return Err(CodexErr::UnsupportedOperation(format!(
                "fallback source uses agent_type '{}', not '{role_name}'",
                route.role_name
            )));
        }
        if route.fallback_claimed {
            return Err(CodexErr::UnsupportedOperation(
                "fallback source has already been consumed".to_string(),
            ));
        }
        route.fallback_claimed = true;
        Ok(route.candidate_index.saturating_add(1))
    }

    pub(crate) fn release_external_backend_fallback(&self, agent_id: ThreadId) {
        if let Some(route) = self
            .external_backend_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&agent_id)
        {
            route.fallback_claimed = false;
        }
    }

    pub(crate) fn rollout_budget(&self) -> &RolloutBudget {
        self.rollout_budget.as_ref()
    }

    /// Send rich user input items to an existing agent thread.
    pub(crate) async fn send_input(
        &self,
        agent_id: ThreadId,
        input: Vec<UserInput>,
        parent_turn_id: Option<String>,
        root_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let state = self.upgrade()?;
        let thread = state.get_thread(agent_id).await?;
        let result = match thread
            .start_or_steer_turn(
                TurnInputRequest::user_input(input).on_start(TurnStartOptions {
                    parent_turn_id,
                    root_turn_id,
                    ..Default::default()
                }),
            )
            .await
        {
            Ok(TurnInputSubmission::Started { turn_id }) => Ok(turn_id),
            Ok(TurnInputSubmission::Steered { .. }) => {
                // MAv1 exposes an opaque `submission_id` to the model. The legacy
                // `Op::UserInput` path returned a fresh ID for every steer, while the
                // turn-input API returns the active turn ID. Keep the tool-visible ID
                // unique without adding a submission receipt back to Core.
                Ok(Uuid::now_v7().to_string())
            }
            Ok(TurnInputSubmission::NotSubmitted { reason }) => Err(CodexErr::InvalidRequest(
                format!("turn input was not submitted: {reason:?}"),
            )),
            Err(err) => Err(err),
        };
        self.handle_thread_request_result(agent_id, &state, result)
            .await
    }

    pub(crate) async fn send_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        agent_communication_context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
        root_turn_id: Option<String>,
    ) -> CodexResult<String> {
        if self.external_agents.contains(agent_id) {
            let (communication_id, _) = self
                .send_external_inter_agent_communication_with_start_hook(
                    agent_id,
                    communication,
                    agent_communication_context,
                    || std::future::ready(()),
                )
                .await?;
            return Ok(communication_id);
        }
        let state = self.upgrade()?;
        if communication.trigger_turn {
            let thread = state.get_thread(agent_id).await?;
            self.ensure_execution_capacity_for_turn_start(&thread)
                .await?;
        }
        self.send_inter_agent_communication_after_capacity_check(
            agent_id,
            &state,
            communication,
            agent_communication_context,
            parent_turn_id,
            root_turn_id,
        )
        .await
    }

    pub(crate) async fn send_external_inter_agent_communication_with_start_hook<F, Fut>(
        &self,
        agent_id: ThreadId,
        communication: InterAgentCommunication,
        agent_communication_context: AgentCommunicationContext,
        on_started: F,
    ) -> CodexResult<(String, bool)>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if !self.external_agents.contains(agent_id) {
            return Err(CodexErr::ThreadNotFound(agent_id));
        }
        if communication.encrypted_content.is_some() {
            return Err(CodexErr::UnsupportedOperation(
                "encrypted inter-agent messages are not supported by external agents".to_string(),
            ));
        }
        let communication_for_log =
            crate::agent_communication::logging_enabled().then(|| communication.clone());
        let submission = self.external_agents.submit_message(
            agent_id,
            communication.content,
            communication.trigger_turn,
        )?;
        if let Some(communication) = communication_for_log {
            crate::agent_communication::emit_agent_communication_send(
                submission.submission_id(),
                &agent_communication_context,
                &communication,
                agent_id,
            );
        }
        let communication_id = submission.submission_id().to_string();
        let requests_turn = submission.requests_turn();
        if submission.starts_generation_now() {
            on_started().await;
            let _ = submission.start();
        } else if requests_turn {
            // 実行中の queued follow-up は次generationの予約にすぎない。
            // Start は現turnのStopのあと、そのgenerationが実際に始まるときに出す。
            submission.defer_start_hook(Box::new(move || Box::pin(on_started())));
            if let Some(pending) = submission.start() {
                pending.start().await;
            }
        } else {
            let _ = submission.start();
        }
        if requests_turn {
            self.maybe_start_external_completion_watcher(agent_id);
        }
        Ok((communication_id, requests_turn))
    }

    pub(super) async fn promote_ready_external_generation(&self, agent_id: ThreadId) {
        if let Some(pending) = self.external_agents.take_ready_pending_start(agent_id) {
            pending.start().await;
        }
    }

    async fn confirm_external_terminal_and_promote(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        child_agent_path: Option<AgentPath>,
        status: &AgentStatus,
        generation: Option<u64>,
        forward_completion: bool,
    ) {
        if forward_completion && let Some(child_agent_path) = child_agent_path {
            self.forward_v2_child_completion_to_parent(
                child_thread_id,
                parent_thread_id,
                child_agent_path,
                status,
            )
            .await;
        }
        if let Some(generation) = generation {
            self.external_agents
                .ack_terminal_observer(child_thread_id, generation);
        }
        self.promote_ready_external_generation(child_thread_id)
            .await;
    }

    async fn send_inter_agent_communication_after_capacity_check(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
        root_turn_id: Option<String>,
    ) -> CodexResult<String> {
        self.submit_inter_agent_communication(
            agent_id,
            state,
            communication,
            context,
            parent_turn_id,
            root_turn_id,
        )
        .await
    }

    async fn submit_inter_agent_communication(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        communication: InterAgentCommunication,
        context: AgentCommunicationContext,
        parent_turn_id: Option<String>,
        root_turn_id: Option<String>,
    ) -> CodexResult<String> {
        let communication_for_log =
            crate::agent_communication::logging_enabled().then(|| communication.clone());
        let parent_turn_id = parent_turn_id.filter(|_| communication.trigger_turn);
        let root_turn_id = root_turn_id.filter(|_| communication.trigger_turn);
        let result = self
            .handle_thread_request_result(
                agent_id,
                state,
                state
                    .send_op(
                        agent_id,
                        Op::InterAgentCommunication { communication },
                        parent_turn_id,
                        root_turn_id,
                    )
                    .await,
            )
            .await;
        if let (Some(communication), Ok(communication_id)) =
            (communication_for_log, result.as_ref())
        {
            crate::agent_communication::emit_agent_communication_send(
                communication_id,
                &context,
                &communication,
                agent_id,
            );
        }
        result
    }

    /// Interrupt the current task for an existing agent thread.
    pub(crate) async fn interrupt_agent(&self, agent_id: ThreadId) -> CodexResult<String> {
        if self.external_agents.contains(agent_id) {
            return self.external_agents.interrupt(agent_id);
        }
        let state = self.upgrade()?;
        self.handle_thread_request_result(
            agent_id,
            &state,
            state
                .send_op(
                    agent_id,
                    Op::Interrupt,
                    /*parent_turn_id*/ None,
                    /*root_turn_id*/ None,
                )
                .await,
        )
        .await
    }

    async fn handle_thread_request_result(
        &self,
        agent_id: ThreadId,
        state: &Arc<ThreadManagerState>,
        result: CodexResult<String>,
    ) -> CodexResult<String> {
        if result
            .as_ref()
            .is_err_and(|err| matches!(err.details(), CodexErrorDetails::InternalAgentDied))
        {
            let _ = state.remove_thread(&agent_id).await;
            self.forget_v2_residency(agent_id);
            self.state.release_spawned_thread(agent_id);
        }
        result
    }

    /// Fetch the last known status for `agent_id`, returning `NotFound` when unavailable.
    pub(crate) async fn get_status(&self, agent_id: ThreadId) -> AgentStatus {
        if let Some(status) = self.external_agents.status(agent_id) {
            return status;
        }
        let Ok(state) = self.upgrade() else {
            // No agent available if upgrade fails.
            return AgentStatus::NotFound;
        };
        let Ok(thread) = state.get_thread(agent_id).await else {
            return AgentStatus::NotFound;
        };
        thread.agent_status().await
    }

    pub(crate) fn register_session_root(
        &self,
        current_thread_id: ThreadId,
        current_parent_thread_id: Option<ThreadId>,
    ) {
        if current_parent_thread_id.is_none() {
            self.state.register_root_thread(current_thread_id);
        }
    }

    pub(crate) fn get_agent_metadata(&self, agent_id: ThreadId) -> Option<AgentMetadata> {
        self.state.agent_metadata_for_thread(agent_id)
    }

    pub(crate) fn ensure_agent_known(&self, agent_id: ThreadId) -> CodexResult<AgentMetadata> {
        self.state
            .agent_metadata_for_thread(agent_id)
            .ok_or_else(|| CodexErr::ThreadNotFound(agent_id))
    }

    pub(crate) async fn list_live_agent_subtree_thread_ids(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<Vec<ThreadId>> {
        let mut thread_ids = vec![agent_id];
        thread_ids.extend(self.live_thread_spawn_descendants(agent_id).await?);
        Ok(thread_ids)
    }

    pub(crate) async fn get_agent_config_snapshot(
        &self,
        agent_id: ThreadId,
    ) -> Option<ThreadConfigSnapshot> {
        if self.external_agents.contains(agent_id) {
            return None;
        }
        let Ok(state) = self.upgrade() else {
            return None;
        };
        let Ok(thread) = state.get_thread(agent_id).await else {
            return None;
        };
        Some(thread.config_snapshot().await)
    }

    pub(crate) async fn resolve_agent_reference(
        &self,
        _current_thread_id: ThreadId,
        current_session_source: &SessionSource,
        agent_reference: &str,
    ) -> CodexResult<ThreadId> {
        let current_agent_path = current_session_source
            .get_agent_path()
            .unwrap_or_else(AgentPath::root);
        let agent_path = current_agent_path
            .resolve(agent_reference)
            .map_err(CodexErr::UnsupportedOperation)?;
        if let Some(thread_id) = self.state.agent_id_for_path(&agent_path) {
            return Ok(thread_id);
        }
        Err(CodexErr::UnsupportedOperation(format!(
            "live agent path `{}` not found",
            agent_path.as_str()
        )))
    }

    /// Subscribe to status updates for `agent_id`, yielding the latest value and changes.
    pub(crate) async fn subscribe_status(
        &self,
        agent_id: ThreadId,
    ) -> CodexResult<watch::Receiver<AgentStatus>> {
        if let Some(status_rx) = self.external_agents.subscribe(agent_id) {
            return Ok(status_rx);
        }
        let state = self.upgrade()?;
        let thread = state.get_thread(agent_id).await?;
        Ok(thread.subscribe_status())
    }

    /// Best-effort live notification for the direct parent of a child agent.
    ///
    /// The notification is deliberately separate from completion communication: it is a
    /// transient UI transition and must not affect parent turn scheduling or model context.
    pub(crate) async fn maybe_notify_parent_of_terminal_status(
        &self,
        child_thread_id: ThreadId,
        status: &AgentStatus,
    ) {
        terminal_notification::maybe_notify_parent_of_terminal_status(
            self,
            child_thread_id,
            status,
        )
        .await;
    }

    pub(crate) async fn format_environment_context_subagents(
        &self,
        parent_thread_id: ThreadId,
    ) -> String {
        let Ok(agents) = self.open_thread_spawn_children(parent_thread_id).await else {
            return String::new();
        };

        agents
            .into_iter()
            .map(|(thread_id, metadata)| {
                let reference = metadata
                    .agent_path
                    .as_ref()
                    .map(|agent_path| agent_path.name().to_string())
                    .unwrap_or_else(|| thread_id.to_string());
                format_subagent_context_line(reference.as_str(), metadata.agent_nickname.as_deref())
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(crate) async fn list_agents(
        &self,
        current_session_source: &SessionSource,
        path_prefix: Option<&str>,
    ) -> CodexResult<Vec<ListedAgent>> {
        let state = self.upgrade()?;
        let resolved_prefix = path_prefix
            .map(|prefix| {
                current_session_source
                    .get_agent_path()
                    .unwrap_or_else(AgentPath::root)
                    .resolve(prefix)
                    .map_err(CodexErr::UnsupportedOperation)
            })
            .transpose()?;

        let mut live_agents = self.state.live_agents();
        live_agents.sort_by(|left, right| {
            left.agent_path
                .as_deref()
                .unwrap_or_default()
                .cmp(right.agent_path.as_deref().unwrap_or_default())
                .then_with(|| {
                    left.agent_id
                        .map(|id| id.to_string())
                        .unwrap_or_default()
                        .cmp(&right.agent_id.map(|id| id.to_string()).unwrap_or_default())
                })
        });

        let root_path = AgentPath::root();
        let mut agents = Vec::with_capacity(live_agents.len().saturating_add(1));
        if resolved_prefix
            .as_ref()
            .is_none_or(|prefix| agent_matches_prefix(Some(&root_path), prefix))
            && let Some(root_thread_id) = self.state.agent_id_for_path(&root_path)
            && let Ok(root_thread) = state.get_thread(root_thread_id).await
        {
            agents.push(ListedAgent {
                agent_name: root_path.to_string(),
                agent_status: root_thread.agent_status().await,
            });
        }

        for metadata in live_agents {
            let Some(thread_id) = metadata.agent_id else {
                continue;
            };
            if resolved_prefix
                .as_ref()
                .is_some_and(|prefix| !agent_matches_prefix(metadata.agent_path.as_ref(), prefix))
            {
                continue;
            }

            let agent_name = metadata
                .agent_path
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| thread_id.to_string());
            if let Some(agent_status) = self.external_agents.status(thread_id) {
                agents.push(ListedAgent {
                    agent_name,
                    agent_status,
                });
            } else if let Ok(thread) = state.get_thread(thread_id).await {
                agents.push(ListedAgent {
                    agent_name,
                    agent_status: thread.agent_status().await,
                });
            }
        }

        Ok(agents)
    }

    /// Starts a detached watcher for sub-agents spawned from another thread.
    ///
    /// This is only enabled for `SubAgentSource::ThreadSpawn`, where a parent thread exists and
    /// can receive completion notifications.
    fn maybe_start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        session_source: Option<SessionSource>,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
    ) {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return;
        };
        self.start_completion_watcher(
            child_thread_id,
            parent_thread_id,
            child_reference,
            child_agent_path,
        );
    }

    fn start_completion_watcher(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        child_reference: String,
        child_agent_path: Option<AgentPath>,
    ) {
        if self.is_external_agent(child_thread_id) {
            let mut watchers = self
                .external_completion_watchers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !watchers.insert(child_thread_id) {
                return;
            }
        }
        let control = self.clone();
        tokio::spawn(async move {
            let is_external = control.is_external_agent(child_thread_id);
            struct ClearExternalWatcher {
                control: AgentControl,
                child_thread_id: ThreadId,
                is_external: bool,
            }
            impl Drop for ClearExternalWatcher {
                fn drop(&mut self) {
                    if self.is_external {
                        self.control
                            .external_completion_watchers
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&self.child_thread_id);
                    }
                }
            }
            let _clear_external_watcher = ClearExternalWatcher {
                control: control.clone(),
                child_thread_id,
                is_external,
            };
            // Native V2 turns emit the live terminal transition from Session so it is adjacent
            // to the typed terminal turn event. Legacy native agents and ACP agents use this
            // watcher as their transition source instead. A missing native child is treated as
            // V2 here, matching the completion path below, so teardown cannot duplicate a
            // notification that was already emitted before the thread disappeared.
            let native_v2 = if is_external {
                false
            } else {
                match control.upgrade() {
                    Ok(state) => match state.get_thread(child_thread_id).await {
                        Ok(child_thread) => {
                            child_thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                        }
                        Err(_) => true,
                    },
                    Err(_) => true,
                }
            };
            let mut terminal_status_tracker =
                terminal_notification::TerminalStatusTracker::default();
            let status = match control.subscribe_status(child_thread_id).await {
                Ok(mut status_rx) => {
                    let mut status = status_rx.borrow().clone();
                    loop {
                        let external_lifecycle = is_external
                            .then(|| control.external_agents.lifecycle_status(child_thread_id))
                            .flatten();
                        if let Some((external_status, _)) = &external_lifecycle {
                            status = external_status.clone();
                        }
                        let external_generation =
                            external_lifecycle.map(|(_, generation)| generation);
                        let should_notify =
                            terminal_status_tracker.should_notify(&status, external_generation);
                        if should_notify && !native_v2 {
                            control
                                .maybe_notify_parent_of_terminal_status(child_thread_id, &status)
                                .await;
                        }
                        if should_notify && is_external {
                            let interrupted = matches!(status, AgentStatus::Interrupted);
                            control
                                .confirm_external_terminal_and_promote(
                                    child_thread_id,
                                    parent_thread_id,
                                    child_agent_path.clone(),
                                    &status,
                                    external_generation,
                                    !interrupted,
                                )
                                .await;
                            if interrupted {
                                return;
                            }
                        }
                        if matches!(status, AgentStatus::Interrupted) {
                            // ACP agents finish their current lifecycle at interruption. Legacy
                            // native agents remain watchable and may transition back to Running.
                            if is_external {
                                return;
                            }
                        } else {
                            let keep_watching_external = is_external
                                && is_final(&status)
                                && !matches!(status, AgentStatus::Shutdown);
                            if keep_watching_external {
                                if status_rx.changed().await.is_err() {
                                    return;
                                }
                                status = status_rx.borrow().clone();
                                continue;
                            }
                        }

                        if is_final(&status) {
                            break status;
                        }

                        if status_rx.changed().await.is_err() {
                            status = control.get_status(child_thread_id).await;
                            if !is_final(&status) {
                                return;
                            }
                        } else {
                            status = status_rx.borrow().clone();
                        }
                    }
                }
                Err(_) => {
                    let mut status = control.get_status(child_thread_id).await;
                    let external_lifecycle = is_external
                        .then(|| control.external_agents.lifecycle_status(child_thread_id))
                        .flatten();
                    if let Some((external_status, _)) = &external_lifecycle {
                        status = external_status.clone();
                    }
                    let external_generation = external_lifecycle.map(|(_, generation)| generation);
                    if terminal_status_tracker.should_notify(&status, external_generation) {
                        if !native_v2 {
                            control
                                .maybe_notify_parent_of_terminal_status(child_thread_id, &status)
                                .await;
                        }
                        if is_external {
                            let interrupted = matches!(status, AgentStatus::Interrupted);
                            control
                                .confirm_external_terminal_and_promote(
                                    child_thread_id,
                                    parent_thread_id,
                                    child_agent_path.clone(),
                                    &status,
                                    external_generation,
                                    !interrupted,
                                )
                                .await;
                            if interrupted {
                                return;
                            }
                        }
                    }
                    if !is_final(&status) {
                        return;
                    }
                    status
                }
            };

            if is_external {
                return;
            }
            let Ok(state) = control.upgrade() else {
                return;
            };
            let child_thread = state.get_thread(child_thread_id).await.ok();
            let child_uses_multi_agent_v2 = match child_thread.as_ref() {
                Some(child_thread) => {
                    child_thread.multi_agent_version() == Some(MultiAgentVersion::V2)
                }
                None => true,
            };
            if child_agent_path.is_some() && child_uses_multi_agent_v2 {
                let Some(child_agent_path) = child_agent_path.clone() else {
                    return;
                };
                control
                    .forward_v2_child_completion_to_parent(
                        child_thread_id,
                        parent_thread_id,
                        child_agent_path,
                        &status,
                    )
                    .await;
                return;
            }
            let message = format_subagent_notification_message(child_reference.as_str(), &status);
            let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
                return;
            };
            parent_thread
                .inject_user_message_without_turn(message)
                .await;
        });
    }

    async fn forward_v2_child_completion_to_parent(
        &self,
        child_thread_id: ThreadId,
        parent_thread_id: ThreadId,
        child_agent_path: AgentPath,
        status: &AgentStatus,
    ) {
        let Some(parent_agent_path) = child_agent_path
            .as_str()
            .rsplit_once('/')
            .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
        else {
            return;
        };
        let Some(message) = format_inter_agent_completion_message(
            parent_agent_path.clone(),
            child_agent_path.clone(),
            status,
        ) else {
            return;
        };
        // Terminal results request a parent turn so an idle parent
        // resumes without wait_agent or new user input.
        let communication = InterAgentCommunication::new(
            child_agent_path,
            parent_agent_path,
            Vec::new(),
            message,
            /*trigger_turn*/ true,
        );
        let context =
            AgentCommunicationContext::new(AgentCommunicationKind::Result, child_thread_id);
        let _ = self
            .send_inter_agent_communication(
                parent_thread_id,
                communication,
                context,
                /*parent_turn_id*/ None,
                /*root_turn_id*/ None,
            )
            .await;
    }

    fn maybe_start_external_completion_watcher(&self, child_thread_id: ThreadId) {
        let Some(metadata) = self.state.agent_metadata_for_thread(child_thread_id) else {
            return;
        };
        let Some(child_agent_path) = metadata.agent_path else {
            return;
        };
        let Some(parent_agent_path) = child_agent_path
            .as_str()
            .rsplit_once('/')
            .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
        else {
            return;
        };
        let Some(parent_thread_id) = self.state.agent_id_for_path(&parent_agent_path) else {
            return;
        };
        self.start_completion_watcher(
            child_thread_id,
            parent_thread_id,
            child_agent_path.to_string(),
            Some(child_agent_path),
        );
    }

    fn prepare_agent_metadata(
        &self,
        reservation: &mut crate::agent::registry::SpawnReservation,
        config: &Config,
        agent_path: Option<AgentPath>,
        agent_role: Option<String>,
        preferred_agent_nickname: Option<String>,
    ) -> CodexResult<AgentMetadata> {
        if let Some(agent_path) = agent_path.as_ref() {
            reservation.reserve_agent_path(agent_path)?;
        }
        let candidate_names = spawn::agent_nickname_candidates(config, agent_role.as_deref());
        let candidate_name_refs: Vec<&str> = candidate_names.iter().map(String::as_str).collect();
        let agent_nickname = Some(reservation.reserve_agent_nickname_with_preference(
            &candidate_name_refs,
            preferred_agent_nickname.as_deref(),
        )?);
        Ok(AgentMetadata {
            agent_id: None,
            agent_path,
            agent_nickname,
            agent_role,
            metadata: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_thread_spawn(
        &self,
        reservation: &mut crate::agent::registry::SpawnReservation,
        config: &Config,
        parent_thread_id: ThreadId,
        depth: i32,
        agent_path: Option<AgentPath>,
        agent_role: Option<String>,
        preferred_agent_nickname: Option<String>,
    ) -> CodexResult<(SessionSource, AgentMetadata)> {
        if depth == 1 {
            self.state.register_root_thread(parent_thread_id);
        }
        let agent_metadata = self.prepare_agent_metadata(
            reservation,
            config,
            agent_path,
            agent_role,
            preferred_agent_nickname,
        )?;
        let session_source = SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id,
            depth,
            agent_path: agent_metadata.agent_path.clone(),
            agent_nickname: agent_metadata.agent_nickname.clone(),
            agent_role: agent_metadata.agent_role.clone(),
        });
        Ok((session_source, agent_metadata))
    }

    fn upgrade(&self) -> CodexResult<Arc<ThreadManagerState>> {
        self.manager
            .upgrade()
            .ok_or_else(|| CodexErr::UnsupportedOperation("thread manager dropped".to_string()))
    }

    async fn inherited_environments_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
    ) -> Option<TurnEnvironmentSnapshot> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        Some(
            parent_thread
                .session
                .services
                .turn_environments
                .snapshot()
                .await,
        )
    }

    async fn inherited_exec_policy_for_source(
        &self,
        state: &Arc<ThreadManagerState>,
        session_source: Option<&SessionSource>,
        child_config: &Config,
    ) -> Option<Arc<crate::exec_policy::ExecPolicyManager>> {
        let Some(SessionSource::SubAgent(SubAgentSource::ThreadSpawn {
            parent_thread_id, ..
        })) = session_source
        else {
            return None;
        };

        let parent_thread = state.get_thread(*parent_thread_id).await.ok()?;
        let parent_config = parent_thread.session.get_config().await;
        if !crate::exec_policy::child_uses_parent_exec_policy(&parent_config, child_config) {
            return None;
        }

        Some(Arc::clone(&parent_thread.session.services.exec_policy))
    }

    async fn open_thread_spawn_children(
        &self,
        parent_thread_id: ThreadId,
    ) -> CodexResult<Vec<(ThreadId, AgentMetadata)>> {
        let mut children_by_parent = self.live_thread_spawn_children().await?;
        Ok(children_by_parent
            .remove(&parent_thread_id)
            .unwrap_or_default())
    }

    async fn live_thread_spawn_children(
        &self,
    ) -> CodexResult<HashMap<ThreadId, Vec<(ThreadId, AgentMetadata)>>> {
        let state = self.upgrade()?;
        let mut children_by_parent = HashMap::<ThreadId, Vec<(ThreadId, AgentMetadata)>>::new();

        for (parent_thread_id, child_thread_id) in state.list_live_thread_spawn_edges().await {
            children_by_parent
                .entry(parent_thread_id)
                .or_default()
                .push((
                    child_thread_id,
                    self.state
                        .agent_metadata_for_thread(child_thread_id)
                        .unwrap_or(AgentMetadata {
                            agent_id: Some(child_thread_id),
                            ..Default::default()
                        }),
                ));
        }

        for metadata in self.state.live_agents() {
            let Some(child_thread_id) = metadata
                .agent_id
                .filter(|id| self.external_agents.contains(*id))
            else {
                continue;
            };
            let Some(parent_path) = metadata
                .agent_path
                .as_ref()
                .and_then(|path| path.as_str().rsplit_once('/').map(|(parent, _)| parent))
                .and_then(|parent| AgentPath::try_from(parent).ok())
            else {
                continue;
            };
            let Some(parent_thread_id) = self.state.agent_id_for_path(&parent_path) else {
                continue;
            };
            children_by_parent
                .entry(parent_thread_id)
                .or_default()
                .push((child_thread_id, metadata));
        }

        for children in children_by_parent.values_mut() {
            children.sort_by(|left, right| {
                left.1
                    .agent_path
                    .as_deref()
                    .unwrap_or_default()
                    .cmp(right.1.agent_path.as_deref().unwrap_or_default())
                    .then_with(|| left.0.to_string().cmp(&right.0.to_string()))
            });
        }

        Ok(children_by_parent)
    }

    async fn persist_thread_spawn_edge_for_source(
        &self,
        child_thread: &crate::CodexThread,
        child_thread_id: ThreadId,
        session_source: Option<&SessionSource>,
    ) {
        let Some(parent_thread_id) = session_source.and_then(SessionSource::parent_thread_id)
        else {
            return;
        };
        if child_thread.config_snapshot().await.ephemeral {
            return;
        }
        let Ok(state) = self.upgrade() else {
            return;
        };
        let Some(agent_graph_store) = state.agent_graph_store() else {
            return;
        };
        if let Err(err) = agent_graph_store
            .upsert_thread_spawn_edge(
                parent_thread_id,
                child_thread_id,
                codex_agent_graph_store::ThreadSpawnEdgeStatus::Open,
            )
            .await
        {
            warn!("failed to persist thread-spawn edge: {err}");
        }
    }

    async fn live_thread_spawn_descendants(
        &self,
        root_thread_id: ThreadId,
    ) -> CodexResult<Vec<ThreadId>> {
        let mut children_by_parent = self.live_thread_spawn_children().await?;
        let mut descendants = Vec::new();
        let mut stack = children_by_parent
            .remove(&root_thread_id)
            .unwrap_or_default()
            .into_iter()
            .map(|(child_thread_id, _)| child_thread_id)
            .rev()
            .collect::<Vec<_>>();

        while let Some(thread_id) = stack.pop() {
            descendants.push(thread_id);
            if let Some(children) = children_by_parent.remove(&thread_id) {
                for (child_thread_id, _) in children.into_iter().rev() {
                    stack.push(child_thread_id);
                }
            }
        }

        Ok(descendants)
    }
}

fn agent_matches_prefix(agent_path: Option<&AgentPath>, prefix: &AgentPath) -> bool {
    if prefix.is_root() {
        return true;
    }

    agent_path.is_some_and(|agent_path| {
        agent_path == prefix
            || agent_path
                .as_str()
                .strip_prefix(prefix.as_str())
                .is_some_and(|suffix| suffix.starts_with('/'))
    })
}

pub(crate) fn render_input_preview(input: &[UserInput]) -> String {
    input
        .iter()
        .map(|item| match item {
            UserInput::Text { text, .. } => text.clone(),
            UserInput::Image { .. } => "[image]".to_string(),
            UserInput::LocalImage { path, .. } => {
                format!("[local_image:{}]", path.display())
            }
            UserInput::Audio { .. } => "[audio]".to_string(),
            UserInput::LocalAudio { path } => {
                format!("[local_audio:{}]", path.display())
            }
            UserInput::Skill { name, path, .. } => {
                format!("[skill:${name}]({})", path.display())
            }
            UserInput::Mention { name, path, .. } => format!("[mention:${name}]({path})"),
            _ => "[input]".to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn thread_spawn_depth(session_source: &SessionSource) -> Option<i32> {
    match session_source {
        SessionSource::SubAgent(SubAgentSource::ThreadSpawn { depth, .. }) => Some(*depth),
        _ => None,
    }
}
#[cfg(test)]
#[path = "control_tests.rs"]
mod tests;
