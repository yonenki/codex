use crate::binding::PendingTeamBinding;
use crate::binding::TeamAgentBinding;
use crate::error::TeamRuntimeError;
use crate::error::TeamRuntimeResult;
use crate::event::TeamEvent;
use crate::event::TeamEventKind;
use crate::event::TeamEventPayload;
use crate::ids::NodeRunId;
use crate::ids::StateRevision;
use crate::ids::TeamSessionId;
use crate::reducer::reduce;
use crate::sink::EnvTeamEventSink;
use crate::sink::TeamEventSink;
use crate::state::TeamLifecycle;
use crate::state::TeamSessionState;
use crate::store::LazySqliteTeamStore;
use crate::store::MemoryTeamStore;
use crate::store::TeamStore;
use chrono::Utc;
use codex_team_graph::GraphSummary;
use codex_team_graph::NodeGuide;
use codex_team_graph::TeamGraph;
use codex_team_graph::TeamGraphCatalog;
use codex_team_graph::ToolCapability;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct TeamControl {
    catalog: Mutex<TeamGraphCatalog>,
    store: Arc<dyn StoreHandle>,
    sink: Arc<dyn ErasedSink>,
    teams: Mutex<BTreeMap<TeamSessionId, TeamSessionState>>,
    bindings: Mutex<BTreeMap<String, TeamAgentBinding>>,
    surface: std::sync::RwLock<SurfaceSnapshot>,
    tool_reporting_agents: Mutex<std::collections::HashSet<String>>,
    restored: tokio::sync::OnceCell<()>,
}

#[derive(Clone, Debug, Default)]
struct SurfaceSnapshot {
    bindings: BTreeMap<String, TeamAgentBinding>,
    available: BTreeMap<String, Vec<ToolCapability>>,
    recommended: BTreeMap<String, Vec<ToolCapability>>,
    open_team_count: usize,
}

trait StoreHandle: Send + Sync {
    fn persist_event(
        &self,
        state: TeamSessionState,
        event: TeamEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>;
    fn load_teams(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamSessionState>>> + Send + '_>,
    >;
    fn pending_outbox(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamEvent>>> + Send + '_>,
    >;
    fn mark_outbox_sent(
        &self,
        event_ids: Vec<crate::ids::EventId>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>;
    fn persist_binding(
        &self,
        binding: TeamAgentBinding,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>;
    fn load_bindings(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamAgentBinding>>> + Send + '_>,
    >;
}

impl<T: TeamStore + 'static> StoreHandle for T {
    fn persist_event(
        &self,
        state: TeamSessionState,
        event: TeamEvent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>
    {
        Box::pin(async move { TeamStore::persist_event(self, &state, &event).await })
    }

    fn load_teams(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamSessionState>>> + Send + '_>,
    > {
        Box::pin(TeamStore::load_teams(self))
    }

    fn pending_outbox(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamEvent>>> + Send + '_>,
    > {
        Box::pin(TeamStore::pending_outbox(self))
    }

    fn mark_outbox_sent(
        &self,
        event_ids: Vec<crate::ids::EventId>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>
    {
        Box::pin(async move { TeamStore::mark_outbox_sent(self, &event_ids).await })
    }

    fn persist_binding(
        &self,
        binding: TeamAgentBinding,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>
    {
        Box::pin(async move { TeamStore::persist_binding(self, &binding).await })
    }

    fn load_bindings(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TeamRuntimeResult<Vec<TeamAgentBinding>>> + Send + '_>,
    > {
        Box::pin(TeamStore::load_bindings(self))
    }
}

trait ErasedSink: Send + Sync {
    fn publish(
        &self,
        events: Vec<TeamEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>;
}

impl<T: TeamEventSink + 'static> ErasedSink for T {
    fn publish(
        &self,
        events: Vec<TeamEvent>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TeamRuntimeResult<()>> + Send + '_>>
    {
        Box::pin(async move { TeamEventSink::publish(self, &events).await })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartTeamCommand {
    pub graph_name: String,
    pub task_ref: Option<String>,
    pub worktree: Option<String>,
    pub branch: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartNodeCommand {
    pub team_session_id: TeamSessionId,
    pub node_id: Option<String>,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordResultCommand {
    pub team_session_id: TeamSessionId,
    pub result: String,
    pub evidence_id: Option<String>,
    pub candidate_sha: Option<String>,
    pub qa: Option<bool>,
    pub findings: Option<u32>,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransitionCommand {
    pub team_session_id: TeamSessionId,
    pub result: String,
    pub deviation_reason: Option<String>,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EndTeamCommand {
    pub team_session_id: TeamSessionId,
    pub aborted: bool,
    pub reason: String,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvidenceCommand {
    pub team_session_id: TeamSessionId,
    pub evidence_id: String,
    pub identity: Option<String>,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExternalWaitCommand {
    pub team_session_id: TeamSessionId,
    pub reason: String,
    pub expected_revision: StateRevision,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NextAction {
    pub tool: ToolCapability,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TeamView {
    pub team_session_id: TeamSessionId,
    pub graph_name: String,
    pub graph_version: String,
    pub graph_hash: String,
    pub revision: StateRevision,
    pub lifecycle: TeamLifecycle,
    pub current_node: Option<NodeGuide>,
    pub last_result: Option<String>,
    pub candidate_sha: Option<String>,
    pub task_ref: Option<String>,
    pub worktree: Option<String>,
    pub branch: Option<String>,
    pub agents: Vec<TeamAgentBinding>,
    pub possible_next: Vec<NextAction>,
    pub recommended_next: Vec<NextAction>,
    pub waiting_reason: Option<String>,
}

impl TeamControl {
    pub fn memory(catalog: TeamGraphCatalog) -> Self {
        Self::new(
            catalog,
            Arc::new(MemoryTeamStore::default()),
            Arc::new(crate::sink::RecordingSink::default()),
        )
    }

    fn new(
        catalog: TeamGraphCatalog,
        store: Arc<dyn StoreHandle>,
        sink: Arc<dyn ErasedSink>,
    ) -> Self {
        Self {
            catalog: Mutex::new(catalog),
            store,
            sink,
            teams: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            surface: std::sync::RwLock::new(SurfaceSnapshot::default()),
            tool_reporting_agents: Mutex::new(std::collections::HashSet::new()),
            restored: tokio::sync::OnceCell::const_new(),
        }
    }

    pub fn open_team_count(&self) -> usize {
        self.surface
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .open_team_count
    }

    pub fn binding_snapshot(&self, agent_thread_id: &str) -> Option<TeamAgentBinding> {
        self.surface
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bindings
            .get(agent_thread_id)
            .cloned()
    }

    pub fn available_tools_for(&self, agent_thread_id: &str) -> Option<Vec<ToolCapability>> {
        let surface = self
            .surface
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let binding = surface.bindings.get(agent_thread_id)?;
        surface
            .available
            .get(binding.team_session_id.as_str())
            .cloned()
    }

    pub fn recommended_tools_for(&self, agent_thread_id: &str) -> Option<Vec<ToolCapability>> {
        let surface = self
            .surface
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let binding = surface.bindings.get(agent_thread_id)?;
        surface
            .recommended
            .get(binding.team_session_id.as_str())
            .cloned()
    }

    pub fn empty() -> Self {
        Self::memory(TeamGraphCatalog::default())
    }

    pub fn team_store_path(codex_home: &Path) -> PathBuf {
        codex_home.join("team-sessions.sqlite")
    }

    /// 標準 Codex 起動が使う永続 store と agent-collab ingest sink。
    pub fn production(codex_home: &Path) -> Self {
        Self::for_codex_home(codex_home, EnvTeamEventSink::from_process_env())
    }

    pub fn for_codex_home(codex_home: &Path, sink: impl TeamEventSink + 'static) -> Self {
        Self::with_store(
            TeamGraphCatalog::default(),
            LazySqliteTeamStore::new(Self::team_store_path(codex_home)),
            sink,
        )
    }

    pub fn with_store(
        catalog: TeamGraphCatalog,
        store: impl TeamStore + 'static,
        sink: impl TeamEventSink + 'static,
    ) -> Self {
        Self::new(catalog, Arc::new(store), Arc::new(sink))
    }

    pub fn with_memory_store(
        catalog: TeamGraphCatalog,
        sink: impl TeamEventSink + 'static,
    ) -> Self {
        Self::with_store(catalog, MemoryTeamStore::default(), sink)
    }

    pub async fn replace_catalog(&self, catalog: TeamGraphCatalog) {
        *self.catalog.lock().await = catalog;
    }

    pub async fn ensure_restored(&self) -> TeamRuntimeResult<()> {
        self.restored
            .get_or_try_init(|| async {
                self.restore().await?;
                match self.flush_outbox().await {
                    Ok(()) | Err(TeamRuntimeError::Sink(_)) => Ok(()),
                    Err(err) => Err(err),
                }
            })
            .await?;
        Ok(())
    }

    pub async fn restore(&self) -> TeamRuntimeResult<()> {
        let teams = self.store.load_teams().await?;
        let bindings = self.store.load_bindings().await?;
        {
            let mut live = self.teams.lock().await;
            live.clear();
            for team in teams {
                live.insert(team.team_session_id.clone(), team);
            }
        }
        {
            let mut live = self.bindings.lock().await;
            live.clear();
            for binding in bindings {
                live.insert(binding.agent_thread_id.clone(), binding);
            }
        }
        self.refresh_surface().await;
        Ok(())
    }

    pub async fn list_graphs(&self) -> Vec<GraphSummary> {
        self.catalog.lock().await.summaries()
    }

    pub async fn get_graph(&self, name: &str) -> TeamRuntimeResult<TeamGraph> {
        self.catalog
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| TeamRuntimeError::invalid(format!("unknown team graph '{name}'")))
    }

    pub async fn list_teams(&self) -> Vec<TeamView> {
        let _ = self.ensure_restored().await;
        let teams = self.teams.lock().await;
        teams.values().map(view_from_state).collect()
    }

    pub async fn has_open_teams(&self) -> bool {
        let _ = self.ensure_restored().await;
        self.teams
            .lock()
            .await
            .values()
            .any(TeamSessionState::is_open)
    }

    pub async fn status(&self, team_session_id: &TeamSessionId) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let teams = self.teams.lock().await;
        teams
            .get(team_session_id)
            .map(view_from_state)
            .ok_or_else(|| TeamRuntimeError::TeamNotFound(team_session_id.clone()))
    }

    pub async fn start_team(&self, command: StartTeamCommand) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let graph = self.get_graph(&command.graph_name).await?;
        let team_session_id = TeamSessionId::generate();
        let mut state = TeamSessionState::start(
            team_session_id.clone(),
            graph.clone(),
            command.task_ref.clone(),
            command.worktree.clone(),
            command.branch.clone(),
        );
        let event = TeamEvent {
            event_id: crate::ids::EventId::generate(),
            team_session_id: team_session_id.clone(),
            sequence: 1,
            kind: TeamEventKind::TeamStarted,
            occurred_at: Utc::now(),
            graph_name: graph.name.clone(),
            graph_version: graph.version.clone(),
            graph_hash: state.graph_hash.clone(),
            node_id: Some(graph.start.clone()),
            node_run_id: None,
            attempt: None,
            agent_thread_id: None,
            role: None,
            payload: TeamEventPayload::TeamStarted {
                task_ref: command.task_ref,
                worktree: command.worktree,
                branch: command.branch,
            },
        };
        self.commit(&mut state, event).await?;
        self.teams
            .lock()
            .await
            .insert(state.team_session_id.clone(), state.clone());
        self.refresh_surface().await;
        Ok(view_from_state(&state))
    }

    pub async fn start_node(&self, command: StartNodeCommand) -> TeamRuntimeResult<TeamView> {
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                if let Some(run) = state.current_node_run.as_ref() {
                    if run.completed_at.is_none() {
                        return Err(TeamRuntimeError::ActiveNodeRunExists(
                            state.team_session_id.clone(),
                        ));
                    }
                }
                let node_id = match command.node_id {
                    Some(id) => id.parse().map_err(TeamRuntimeError::invalid)?,
                    None => state.current_node_id.clone(),
                };
                if node_id != state.current_node_id {
                    return Err(TeamRuntimeError::invalid(format!(
                        "cannot start node '{node_id}' while current node is '{}'",
                        state.current_node_id
                    )));
                }
                let node = state
                    .graph
                    .node(&node_id)
                    .ok_or_else(|| TeamRuntimeError::invalid(format!("unknown node '{node_id}'")))?
                    .clone();
                let attempt = state
                    .current_node_run
                    .as_ref()
                    .filter(|run| run.node_id == node_id)
                    .map(|run| run.attempt.saturating_add(1))
                    .unwrap_or(1);
                let node_run_id = NodeRunId::generate();
                Ok(TeamEvent {
                    event_id: crate::ids::EventId::generate(),
                    team_session_id: state.team_session_id.clone(),
                    sequence: state.next_sequence,
                    kind: TeamEventKind::NodeStarted,
                    occurred_at: Utc::now(),
                    graph_name: state.graph.name.clone(),
                    graph_version: state.graph.version.clone(),
                    graph_hash: state.graph_hash.clone(),
                    node_id: Some(node_id),
                    node_run_id: Some(node_run_id),
                    attempt: Some(attempt),
                    agent_thread_id: None,
                    role: node.role.as_ref().map(ToString::to_string),
                    payload: TeamEventPayload::NodeStarted {
                        purpose: node.purpose,
                    },
                })
            },
        )
        .await
    }

    pub async fn record_result(&self, command: RecordResultCommand) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let recommended = {
            let teams = self.teams.lock().await;
            let state = teams
                .get(&command.team_session_id)
                .ok_or_else(|| TeamRuntimeError::TeamNotFound(command.team_session_id.clone()))?;
            require_open(state)?;
            require_active_node_run(state)?;
            state.graph.node(&state.current_node_id).and_then(|node| {
                node.transition_for(&command.result)
                    .filter(|transition| transition.recommended)
                    .map(|transition| (command.result.clone(), transition.to.to_string()))
            })
        };
        let view = self
            .mutate(
                command.team_session_id.clone(),
                command.expected_revision,
                |state| {
                    require_active_node_run(state)?;
                    Ok(TeamEvent {
                        event_id: crate::ids::EventId::generate(),
                        team_session_id: state.team_session_id.clone(),
                        sequence: state.next_sequence,
                        kind: TeamEventKind::NodeCompleted,
                        occurred_at: Utc::now(),
                        graph_name: state.graph.name.clone(),
                        graph_version: state.graph.version.clone(),
                        graph_hash: state.graph_hash.clone(),
                        node_id: Some(state.current_node_id.clone()),
                        node_run_id: state
                            .current_node_run
                            .as_ref()
                            .map(|run| run.node_run_id.clone()),
                        attempt: state.current_node_run.as_ref().map(|run| run.attempt),
                        agent_thread_id: None,
                        role: None,
                        payload: TeamEventPayload::NodeCompleted {
                            result: command.result,
                            candidate_sha: command.candidate_sha,
                            evidence_id: command.evidence_id,
                            qa: command.qa,
                            findings: command.findings,
                        },
                    })
                },
            )
            .await?;
        match recommended {
            Some((result, to)) => {
                self.emit_transition_recommended(&view.team_session_id, &result, &to)
                    .await
            }
            None => Ok(view),
        }
    }

    pub async fn next(&self, team_session_id: &TeamSessionId) -> TeamRuntimeResult<TeamView> {
        self.status(team_session_id).await
    }

    pub async fn transition(&self, command: TransitionCommand) -> TeamRuntimeResult<TeamView> {
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                let run = state.current_node_run.as_ref().ok_or_else(|| {
                    TeamRuntimeError::NoCompletedNodeRun(state.team_session_id.clone())
                })?;
                if run.completed_at.is_none() {
                    return Err(TeamRuntimeError::NoCompletedNodeRun(
                        state.team_session_id.clone(),
                    ));
                }
                if run.result.as_deref() != Some(&command.result) {
                    return Err(TeamRuntimeError::invalid(format!(
                        "node completed with result '{:?}', cannot transition on '{}'",
                        run.result, command.result
                    )));
                }
                let node = state.graph.node(&state.current_node_id).ok_or_else(|| {
                    TeamRuntimeError::invalid("current node missing from graph snapshot")
                })?;
                let transition = node.transition_for(&command.result).ok_or_else(|| {
                    TeamRuntimeError::invalid(format!(
                        "result '{}' is not a declared transition from '{}'",
                        command.result, state.current_node_id
                    ))
                })?;
                let recommended = transition.recommended;
                if !recommended && command.deviation_reason.is_none() {
                    return Err(TeamRuntimeError::invalid(
                        "non-recommended transition requires deviation_reason",
                    ));
                }
                Ok(TeamEvent {
                    event_id: crate::ids::EventId::generate(),
                    team_session_id: state.team_session_id.clone(),
                    sequence: state.next_sequence,
                    kind: TeamEventKind::TransitionSelected,
                    occurred_at: Utc::now(),
                    graph_name: state.graph.name.clone(),
                    graph_version: state.graph.version.clone(),
                    graph_hash: state.graph_hash.clone(),
                    node_id: Some(state.current_node_id.clone()),
                    node_run_id: state
                        .current_node_run
                        .as_ref()
                        .map(|run| run.node_run_id.clone()),
                    attempt: state.current_node_run.as_ref().map(|run| run.attempt),
                    agent_thread_id: None,
                    role: None,
                    payload: TeamEventPayload::Transition {
                        result: Some(command.result),
                        to: Some(transition.to.to_string()),
                        recommended,
                        deviation_reason: command.deviation_reason,
                        metric_effects: transition.metric_effects.clone(),
                    },
                })
            },
        )
        .await
    }

    pub async fn end_team(&self, command: EndTeamCommand) -> TeamRuntimeResult<TeamView> {
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                if !command.aborted {
                    if !state.graph.is_terminal(&state.current_node_id) {
                        return Err(TeamRuntimeError::NonTerminalNode {
                            team: state.team_session_id.clone(),
                            node: state.current_node_id.clone(),
                        });
                    }
                    if state
                        .current_node_run
                        .as_ref()
                        .is_some_and(|run| run.completed_at.is_none())
                    {
                        return Err(TeamRuntimeError::ActiveNodeRunExists(
                            state.team_session_id.clone(),
                        ));
                    }
                    if !state.agents.is_empty() {
                        return Err(TeamRuntimeError::ActiveAgents(
                            state.team_session_id.clone(),
                        ));
                    }
                }
                Ok(TeamEvent {
                    event_id: crate::ids::EventId::generate(),
                    team_session_id: state.team_session_id.clone(),
                    sequence: state.next_sequence,
                    kind: if command.aborted {
                        TeamEventKind::TeamAborted
                    } else {
                        TeamEventKind::TeamCompleted
                    },
                    occurred_at: Utc::now(),
                    graph_name: state.graph.name.clone(),
                    graph_version: state.graph.version.clone(),
                    graph_hash: state.graph_hash.clone(),
                    node_id: Some(state.current_node_id.clone()),
                    node_run_id: None,
                    attempt: None,
                    agent_thread_id: None,
                    role: None,
                    payload: TeamEventPayload::TeamClosed {
                        reason: command.reason,
                    },
                })
            },
        )
        .await
    }

    pub async fn binding_for(&self, agent_thread_id: &str) -> Option<TeamAgentBinding> {
        self.bindings.lock().await.get(agent_thread_id).cloned()
    }

    /// Resolve a caller binding only after the persistent Team authority has been restored.
    /// Callers making authorization decisions must not interpret a restore failure as unbound.
    pub async fn binding_for_checked(
        &self,
        agent_thread_id: &str,
    ) -> TeamRuntimeResult<Option<TeamAgentBinding>> {
        self.ensure_restored().await?;
        Ok(self.binding_for(agent_thread_id).await)
    }

    pub async fn require_same_team(
        &self,
        team_session_id: &TeamSessionId,
        agent_thread_id: &str,
    ) -> TeamRuntimeResult<TeamAgentBinding> {
        let binding = self.binding_for(agent_thread_id).await.ok_or_else(|| {
            TeamRuntimeError::invalid(format!("agent '{agent_thread_id}' is not team-bound"))
        })?;
        if binding.team_session_id != *team_session_id {
            return Err(TeamRuntimeError::CrossTeamRef {
                team: team_session_id.clone(),
                subject: agent_thread_id.to_string(),
            });
        }
        Ok(binding)
    }

    pub async fn pending_binding_for_node(
        &self,
        team_session_id: &TeamSessionId,
        role: &str,
    ) -> TeamRuntimeResult<PendingTeamBinding> {
        self.ensure_restored().await?;
        let teams = self.teams.lock().await;
        let state = teams
            .get(team_session_id)
            .ok_or_else(|| TeamRuntimeError::TeamNotFound(team_session_id.clone()))?;
        if !state.is_open() {
            return Err(TeamRuntimeError::ClosedTeam(team_session_id.clone()));
        }
        let run = require_active_node_run(state)?;
        let node = state
            .graph
            .node(&run.node_id)
            .ok_or_else(|| TeamRuntimeError::invalid(format!("unknown node '{}'", run.node_id)))?;
        match node.role.as_ref() {
            Some(expected) if expected.as_str() != role => {
                return Err(TeamRuntimeError::RoleMismatch {
                    expected: expected.to_string(),
                    actual: role.to_string(),
                });
            }
            Some(_) => {}
            None => {
                return Err(TeamRuntimeError::invalid(
                    "current node does not declare a Role for team.spawn_agent",
                ));
            }
        }
        Ok(PendingTeamBinding {
            team_session_id: team_session_id.clone(),
            node_run_id: run.node_run_id.clone(),
            node_id: run.node_id.clone(),
            role: role.to_string(),
            backend_fallback: false,
        })
    }

    pub async fn bind_agent_before_start(
        &self,
        agent_thread_id: impl Into<String>,
        pending: PendingTeamBinding,
    ) -> TeamRuntimeResult<TeamAgentBinding> {
        let agent_thread_id = agent_thread_id.into();
        let backend_fallback = pending.backend_fallback;
        let binding = pending.bind(agent_thread_id.clone());
        self.store.persist_binding(binding.clone()).await?;
        {
            let mut bindings = self.bindings.lock().await;
            bindings.insert(agent_thread_id.clone(), binding.clone());
        }
        self.mutate_without_cas(binding.team_session_id.clone(), |state| {
            if state.node_run(&binding.node_run_id).is_none() {
                return Err(TeamRuntimeError::CrossTeamRef {
                    team: state.team_session_id.clone(),
                    subject: binding.node_run_id.to_string(),
                });
            }
            Ok(TeamEvent {
                event_id: crate::ids::EventId::generate(),
                team_session_id: state.team_session_id.clone(),
                sequence: state.next_sequence,
                kind: TeamEventKind::AgentAttached,
                occurred_at: Utc::now(),
                graph_name: state.graph.name.clone(),
                graph_version: state.graph.version.clone(),
                graph_hash: state.graph_hash.clone(),
                node_id: Some(binding.node_id.clone()),
                node_run_id: Some(binding.node_run_id.clone()),
                attempt: state.current_node_run.as_ref().map(|run| run.attempt),
                agent_thread_id: Some(agent_thread_id),
                role: Some(binding.role.clone()),
                payload: TeamEventPayload::AgentAttached {
                    role: binding.role.clone(),
                    backend_fallback: backend_fallback.then_some(true),
                },
            })
        })
        .await?;
        Ok(binding)
    }

    pub async fn record_agent_terminal(
        &self,
        agent_thread_id: &str,
        status: &str,
    ) -> TeamRuntimeResult<()> {
        let Some(binding) = self.binding_for(agent_thread_id).await else {
            return Ok(());
        };
        let reported = self
            .tool_reporting_agents
            .lock()
            .await
            .contains(agent_thread_id);
        if !reported {
            let _ = self
                .record_tool_operation(
                    agent_thread_id,
                    "acp",
                    "unreported",
                    crate::event::TeamEventKind::ToolCoverageUnreported,
                    Some("unreported"),
                )
                .await;
        }
        let kind = if status == "interrupted" {
            TeamEventKind::AgentInterrupted
        } else {
            TeamEventKind::AgentCompleted
        };
        self.mutate_without_cas(binding.team_session_id, |state| {
            Ok(TeamEvent {
                event_id: crate::ids::EventId::generate(),
                team_session_id: state.team_session_id.clone(),
                sequence: state.next_sequence,
                kind,
                occurred_at: Utc::now(),
                graph_name: state.graph.name.clone(),
                graph_version: state.graph.version.clone(),
                graph_hash: state.graph_hash.clone(),
                node_id: Some(binding.node_id.clone()),
                node_run_id: Some(binding.node_run_id.clone()),
                attempt: None,
                agent_thread_id: Some(binding.agent_thread_id.clone()),
                role: Some(binding.role.clone()),
                payload: TeamEventPayload::AgentTerminal {
                    status: status.to_string(),
                },
            })
        })
        .await?;
        Ok(())
    }

    pub async fn record_tool_operation(
        &self,
        agent_thread_id: &str,
        tool_name: &str,
        call_id: &str,
        kind: TeamEventKind,
        coverage: Option<&str>,
    ) -> TeamRuntimeResult<()> {
        let Some(binding) = self.binding_for(agent_thread_id).await else {
            return Ok(());
        };
        if kind != crate::event::TeamEventKind::ToolCoverageUnreported {
            self.tool_reporting_agents
                .lock()
                .await
                .insert(agent_thread_id.to_string());
        }
        self.mutate_without_cas(binding.team_session_id, |state| {
            Ok(TeamEvent {
                event_id: crate::ids::EventId::generate(),
                team_session_id: state.team_session_id.clone(),
                sequence: state.next_sequence,
                kind,
                occurred_at: Utc::now(),
                graph_name: state.graph.name.clone(),
                graph_version: state.graph.version.clone(),
                graph_hash: state.graph_hash.clone(),
                node_id: Some(binding.node_id.clone()),
                node_run_id: Some(binding.node_run_id.clone()),
                attempt: None,
                agent_thread_id: Some(binding.agent_thread_id.clone()),
                role: Some(binding.role.clone()),
                payload: TeamEventPayload::ToolOperation {
                    tool_name: tool_name.to_string(),
                    call_id: call_id.to_string(),
                    coverage: coverage.map(str::to_owned),
                },
            })
        })
        .await?;
        Ok(())
    }

    pub async fn record_deviation(
        &self,
        team_session_id: &TeamSessionId,
        reason: &str,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate_without_cas(team_session_id.clone(), |state| {
            Ok(TeamEvent {
                event_id: crate::ids::EventId::generate(),
                team_session_id: state.team_session_id.clone(),
                sequence: state.next_sequence,
                kind: TeamEventKind::DeviationRecorded,
                occurred_at: Utc::now(),
                graph_name: state.graph.name.clone(),
                graph_version: state.graph.version.clone(),
                graph_hash: state.graph_hash.clone(),
                node_id: Some(state.current_node_id.clone()),
                node_run_id: state
                    .current_node_run
                    .as_ref()
                    .map(|run| run.node_run_id.clone()),
                attempt: None,
                agent_thread_id: None,
                role: None,
                payload: TeamEventPayload::Transition {
                    result: None,
                    to: None,
                    recommended: false,
                    deviation_reason: Some(reason.to_string()),
                    metric_effects: Vec::new(),
                },
            })
        })
        .await
    }

    pub async fn record_evidence(
        &self,
        command: EvidenceCommand,
        kind: TeamEventKind,
    ) -> TeamRuntimeResult<TeamView> {
        if !matches!(
            kind,
            TeamEventKind::EvidenceRecorded
                | TeamEventKind::EvidenceInvalidated
                | TeamEventKind::EvidenceReused
        ) {
            return Err(TeamRuntimeError::invalid(
                "evidence command requires an evidence event kind",
            ));
        }
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                Ok(event_from_state(
                    state,
                    kind,
                    TeamEventPayload::Evidence {
                        evidence_id: command.evidence_id,
                        identity: command.identity,
                    },
                ))
            },
        )
        .await
    }

    pub async fn enter_external_wait(
        &self,
        command: ExternalWaitCommand,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                Ok(event_from_state(
                    state,
                    TeamEventKind::ExternalWaitEntered,
                    TeamEventPayload::ExternalWait {
                        reason: command.reason,
                    },
                ))
            },
        )
        .await
    }

    pub async fn resolve_external_wait(
        &self,
        command: ExternalWaitCommand,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate(
            command.team_session_id,
            command.expected_revision,
            |state| {
                Ok(event_from_state(
                    state,
                    TeamEventKind::ExternalWaitResolved,
                    TeamEventPayload::ExternalWait {
                        reason: command.reason,
                    },
                ))
            },
        )
        .await
    }

    pub async fn record_agent_wait_entered(
        &self,
        team_session_id: &TeamSessionId,
        target: &str,
        reason: &str,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate_without_cas(team_session_id.clone(), |state| {
            Ok(event_from_state(
                state,
                TeamEventKind::AgentWaitEntered,
                TeamEventPayload::AgentWait {
                    target: target.to_string(),
                    reason: reason.to_string(),
                },
            ))
        })
        .await
    }

    pub async fn record_agent_wait_resolved(
        &self,
        team_session_id: &TeamSessionId,
        target: &str,
        reason: &str,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate_without_cas(team_session_id.clone(), |state| {
            Ok(event_from_state(
                state,
                TeamEventKind::AgentWaitResolved,
                TeamEventPayload::AgentWait {
                    target: target.to_string(),
                    reason: reason.to_string(),
                },
            ))
        })
        .await
    }

    pub async fn flush_outbox(&self) -> TeamRuntimeResult<()> {
        let pending = self.store.pending_outbox().await?;
        if pending.is_empty() {
            return Ok(());
        }
        for chunk in pending.chunks(crate::TEAM_EVENTS_MAX_BATCH) {
            self.sink.publish(chunk.to_vec()).await?;
            let ids = chunk.iter().map(|event| event.event_id.clone()).collect();
            self.store.mark_outbox_sent(ids).await?;
        }
        Ok(())
    }

    async fn emit_transition_recommended(
        &self,
        team_session_id: &TeamSessionId,
        result: &str,
        to: &str,
    ) -> TeamRuntimeResult<TeamView> {
        self.mutate_without_cas(team_session_id.clone(), |state| {
            Ok(event_from_state(
                state,
                TeamEventKind::TransitionRecommended,
                TeamEventPayload::Transition {
                    result: Some(result.to_string()),
                    to: Some(to.to_string()),
                    recommended: true,
                    deviation_reason: None,
                    metric_effects: Vec::new(),
                },
            ))
        })
        .await
    }

    async fn mutate(
        &self,
        team_session_id: TeamSessionId,
        expected_revision: StateRevision,
        build: impl FnOnce(&mut TeamSessionState) -> TeamRuntimeResult<TeamEvent>,
    ) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let mut teams = self.teams.lock().await;
        let state = teams
            .get_mut(&team_session_id)
            .ok_or_else(|| TeamRuntimeError::TeamNotFound(team_session_id.clone()))?;
        require_open(state)?;
        if state.revision != expected_revision {
            return Err(TeamRuntimeError::StaleRevision {
                expected: expected_revision,
                actual: state.revision,
            });
        }
        let event = build(state)?;
        self.commit(state, event).await?;
        let view = view_from_state(state);
        drop(teams);
        self.refresh_surface().await;
        Ok(view)
    }

    async fn mutate_without_cas(
        &self,
        team_session_id: TeamSessionId,
        build: impl FnOnce(&mut TeamSessionState) -> TeamRuntimeResult<TeamEvent>,
    ) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let mut teams = self.teams.lock().await;
        let state = teams
            .get_mut(&team_session_id)
            .ok_or_else(|| TeamRuntimeError::TeamNotFound(team_session_id.clone()))?;
        let event = build(state)?;
        self.commit(state, event).await?;
        let view = view_from_state(state);
        drop(teams);
        self.refresh_surface().await;
        Ok(view)
    }

    async fn commit(
        &self,
        state: &mut TeamSessionState,
        event: TeamEvent,
    ) -> TeamRuntimeResult<()> {
        let mut next = state.clone();
        if event.sequence == 1 && event.kind == TeamEventKind::TeamStarted {
            // start_team already constructed the initial snapshot.
        } else {
            reduce(&mut next, &event)?;
        }
        self.store
            .persist_event(next.clone(), event.clone())
            .await?;
        *state = next;
        match self.flush_outbox().await {
            Ok(()) | Err(TeamRuntimeError::Sink(_)) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn refresh_surface(&self) {
        let teams = self.teams.lock().await;
        let bindings = self.bindings.lock().await;
        let mut available = BTreeMap::new();
        let mut recommended = BTreeMap::new();
        for (id, state) in teams.iter() {
            if let Some(node) = state.graph.node(&state.current_node_id) {
                available.insert(id.to_string(), node.available_tools.clone());
                recommended.insert(id.to_string(), node.recommended_tools.clone());
            }
        }
        *self
            .surface
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = SurfaceSnapshot {
            bindings: bindings.clone(),
            available,
            recommended,
            open_team_count: teams.values().filter(|team| team.is_open()).count(),
        };
    }
}

fn require_open(state: &TeamSessionState) -> TeamRuntimeResult<()> {
    if state.is_open() {
        Ok(())
    } else {
        Err(TeamRuntimeError::ClosedTeam(state.team_session_id.clone()))
    }
}

fn require_active_node_run(state: &TeamSessionState) -> TeamRuntimeResult<&crate::state::NodeRun> {
    match state.current_node_run.as_ref() {
        Some(run) if run.completed_at.is_none() => Ok(run),
        _ => Err(TeamRuntimeError::NoActiveNodeRun(
            state.team_session_id.clone(),
        )),
    }
}

fn event_from_state(
    state: &TeamSessionState,
    kind: TeamEventKind,
    payload: TeamEventPayload,
) -> TeamEvent {
    TeamEvent {
        event_id: crate::ids::EventId::generate(),
        team_session_id: state.team_session_id.clone(),
        sequence: state.next_sequence,
        kind,
        occurred_at: Utc::now(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.clone(),
        node_id: Some(state.current_node_id.clone()),
        node_run_id: state
            .current_node_run
            .as_ref()
            .map(|run| run.node_run_id.clone()),
        attempt: state.current_node_run.as_ref().map(|run| run.attempt),
        agent_thread_id: None,
        role: None,
        payload,
    }
}

fn view_from_state(state: &TeamSessionState) -> TeamView {
    let current_node = state
        .graph
        .node(&state.current_node_id)
        .map(NodeGuide::from_node);
    let (possible_next, recommended_next) = next_actions(state, current_node.as_ref());
    TeamView {
        team_session_id: state.team_session_id.clone(),
        graph_name: state.graph.name.clone(),
        graph_version: state.graph.version.clone(),
        graph_hash: state.graph_hash.to_string(),
        revision: state.revision,
        lifecycle: state.lifecycle,
        current_node,
        last_result: state.last_result.clone(),
        candidate_sha: state.candidate_sha.clone(),
        task_ref: state.task_ref.clone(),
        worktree: state.worktree.clone(),
        branch: state.branch.clone(),
        agents: state.agents.values().cloned().collect(),
        possible_next,
        recommended_next,
        waiting_reason: state.waiting_reason.clone(),
    }
}

fn next_actions(
    state: &TeamSessionState,
    guide: Option<&NodeGuide>,
) -> (Vec<NextAction>, Vec<NextAction>) {
    if !state.is_open() {
        return (Vec::new(), Vec::new());
    }
    let Some(guide) = guide else {
        return (Vec::new(), Vec::new());
    };
    let mut possible = Vec::new();
    let mut recommended = Vec::new();

    if state.graph.is_terminal(&state.current_node_id) {
        possible.push(NextAction {
            tool: ToolCapability::EndTeam,
            reason: "Close the completed team session.".to_string(),
        });
        if state.agents.is_empty()
            && state
                .current_node_run
                .as_ref()
                .is_none_or(|run| run.completed_at.is_some())
        {
            recommended.push(NextAction {
                tool: ToolCapability::EndTeam,
                reason: "Terminal node reached with no active runs or agents.".to_string(),
            });
            return (possible, recommended);
        }
    }

    if state.current_node_run.is_none() {
        possible.push(NextAction {
            tool: ToolCapability::StartTeamNode,
            reason: "Start the current node run before spawning agents.".to_string(),
        });
        recommended.push(possible[0].clone());
        return (possible, recommended);
    }
    if guide.role.is_some() {
        possible.push(NextAction {
            tool: ToolCapability::SpawnAgent,
            reason: "Spawn the node role through team.spawn_agent.".to_string(),
        });
        recommended.push(NextAction {
            tool: ToolCapability::SpawnAgent,
            reason: "The node declares a Role.".to_string(),
        });
    }
    possible.push(NextAction {
        tool: ToolCapability::RecordTeamResult,
        reason: "Record the structured node result.".to_string(),
    });
    possible.push(NextAction {
        tool: ToolCapability::GetTeamNext,
        reason: "Inspect possible and recommended transitions.".to_string(),
    });
    possible.push(NextAction {
        tool: ToolCapability::TransitionTeam,
        reason: "Advance to a declared successor node.".to_string(),
    });
    if state.last_result.is_some() {
        recommended.push(NextAction {
            tool: ToolCapability::TransitionTeam,
            reason: "A node result is recorded.".to_string(),
        });
    } else {
        recommended.push(NextAction {
            tool: ToolCapability::RecordTeamResult,
            reason: "The current node has no recorded result.".to_string(),
        });
    }
    (possible, recommended)
}
