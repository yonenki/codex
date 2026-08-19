use super::*;
use crate::agent::role::ResolvedExternalAgentBackend;
use agent_client_protocol::AcpAgent;
use agent_client_protocol::AcpAgentConfig;
use agent_client_protocol::Agent;
use agent_client_protocol::ConnectionTo;
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::CancelNotification;
use agent_client_protocol::schema::v1::ContentBlock;
use agent_client_protocol::schema::v1::ContentChunk;
use agent_client_protocol::schema::v1::InitializeRequest;
use agent_client_protocol::schema::v1::NewSessionRequest;
use agent_client_protocol::schema::v1::PermissionOptionKind;
use agent_client_protocol::schema::v1::PromptRequest;
use agent_client_protocol::schema::v1::RequestPermissionOutcome;
use agent_client_protocol::schema::v1::RequestPermissionRequest;
use agent_client_protocol::schema::v1::RequestPermissionResponse;
use agent_client_protocol::schema::v1::SelectedPermissionOutcome;
use agent_client_protocol::schema::v1::SessionNotification;
use agent_client_protocol::schema::v1::SessionUpdate;
use agent_client_protocol::schema::v1::StopReason;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::Duration;
use tokio::time::timeout;

pub(super) type GenerationStartHook =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

const MAX_RESULT_TOKENS: usize = 8_000;
const ACP_CANCEL_GRACE_PERIOD: Duration = Duration::from_secs(5);

#[derive(Default)]
pub(super) struct ExternalAgentManager {
    agents: Mutex<HashMap<ThreadId, Arc<ExternalAgent>>>,
}

struct ExternalAgent {
    identity: ExternalAgentIdentity,
    command_tx: async_channel::Sender<AcpCommand>,
    runtime: Mutex<ExternalAgentRuntime>,
    status_tx: watch::Sender<AgentStatus>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ExternalAgentIdentity {
    pub(super) harness: String,
    pub(super) model: Option<String>,
}

#[derive(Default)]
struct ExternalAgentRuntime {
    running: bool,
    generation: u64,
    queued_messages: VecDeque<QueuedMessage>,
    active_task: Option<AbortHandle>,
}

struct QueuedMessage {
    content: String,
    trigger_turn: bool,
    submission_id: Option<String>,
    ready_to_start: bool,
    on_started: Option<GenerationStartHook>,
}

pub(super) struct ExternalMessageSubmission {
    submission_id: String,
    agent: Arc<ExternalAgent>,
    action: Option<ExternalMessageSubmissionAction>,
}

enum ExternalMessageSubmissionAction {
    QueueOnly,
    StartTurn {
        queued_messages: VecDeque<QueuedMessage>,
        content: String,
        generation: u64,
    },
    ReleaseQueuedTurn {
        submission_id: String,
    },
}

impl ExternalMessageSubmission {
    pub(super) fn submission_id(&self) -> &str {
        &self.submission_id
    }

    pub(super) fn requests_turn(&self) -> bool {
        matches!(
            self.action.as_ref(),
            Some(
                ExternalMessageSubmissionAction::StartTurn { .. }
                    | ExternalMessageSubmissionAction::ReleaseQueuedTurn { .. }
            )
        )
    }

    pub(super) fn starts_generation_now(&self) -> bool {
        matches!(
            self.action.as_ref(),
            Some(ExternalMessageSubmissionAction::StartTurn { .. })
        )
    }

    pub(super) fn defer_start_hook(&self, hook: GenerationStartHook) {
        let mut runtime = self
            .agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(message) = runtime
            .queued_messages
            .iter_mut()
            .find(|message| message.submission_id.as_deref() == Some(self.submission_id.as_str()))
        {
            message.on_started = Some(hook);
        }
    }

    pub(super) fn start(mut self) -> Option<PendingGenerationStart> {
        let Some(action) = self.action.take() else {
            return None;
        };
        match action {
            ExternalMessageSubmissionAction::QueueOnly => None,
            ExternalMessageSubmissionAction::StartTurn {
                mut queued_messages,
                content,
                generation,
            } => {
                let prompt = prepend_queued_messages(&mut queued_messages, content);
                start_turn(Arc::clone(&self.agent), prompt, generation);
                None
            }
            ExternalMessageSubmissionAction::ReleaseQueuedTurn { submission_id } => {
                release_queued_turn(Arc::clone(&self.agent), &submission_id)
            }
        }
    }
}

pub(super) struct PendingGenerationStart {
    agent: Arc<ExternalAgent>,
    prompt: String,
    generation: u64,
    on_started: Option<GenerationStartHook>,
    started: bool,
}

impl PendingGenerationStart {
    pub(super) async fn start(mut self) {
        if let Some(hook) = self.on_started.take() {
            hook().await;
        }
        self.started = true;
        start_turn(
            Arc::clone(&self.agent),
            self.prompt.clone(),
            self.generation,
        );
    }
}

impl Drop for PendingGenerationStart {
    fn drop(&mut self) {
        if self.started {
            return;
        }
        let _ = self.on_started.take();
        rollback_reserved_turn(Arc::clone(&self.agent), VecDeque::new(), self.generation);
    }
}

impl Drop for ExternalMessageSubmission {
    fn drop(&mut self) {
        let Some(action) = self.action.take() else {
            return;
        };
        match action {
            ExternalMessageSubmissionAction::QueueOnly => {}
            ExternalMessageSubmissionAction::StartTurn {
                queued_messages,
                generation,
                ..
            } => {
                rollback_reserved_turn(Arc::clone(&self.agent), queued_messages, generation);
            }
            ExternalMessageSubmissionAction::ReleaseQueuedTurn { submission_id } => {
                rollback_queued_turn(Arc::clone(&self.agent), &submission_id);
            }
        }
    }
}

enum AcpCommand {
    Prompt {
        prompt: String,
        response: oneshot::Sender<AgentStatus>,
    },
    Cancel,
    Shutdown,
}

impl ExternalAgentManager {
    pub(super) fn contains(&self, agent_id: ThreadId) -> bool {
        self.agent(agent_id).is_some()
    }

    pub(super) fn register(
        &self,
        agent_id: ThreadId,
        backend: ResolvedExternalAgentBackend,
        cwd: std::path::PathBuf,
        env: HashMap<String, String>,
    ) -> CodexResult<()> {
        let (status_tx, _) = watch::channel(AgentStatus::PendingInit);
        let (command_tx, command_rx) = async_channel::unbounded();
        let agent = Arc::new(ExternalAgent {
            identity: ExternalAgentIdentity {
                harness: backend.harness.clone(),
                model: backend.model.clone(),
            },
            command_tx,
            runtime: Mutex::new(ExternalAgentRuntime::default()),
            status_tx: status_tx.clone(),
        });
        let mut agents = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if agents.insert(agent_id, Arc::clone(&agent)).is_some() {
            return Err(CodexErr::Fatal(format!(
                "external agent id {agent_id} was registered twice"
            )));
        }
        tokio::spawn(async move {
            if let Err(error) = run_acp_agent(backend, cwd, env, command_rx).await {
                status_tx.send_replace(AgentStatus::Errored(error));
            }
        });
        Ok(())
    }

    pub(super) fn remove(&self, agent_id: ThreadId) {
        if let Some(agent) = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&agent_id)
        {
            let _ = agent.command_tx.try_send(AcpCommand::Shutdown);
        }
    }

    pub(super) fn status(&self, agent_id: ThreadId) -> Option<AgentStatus> {
        self.agent(agent_id)
            .map(|agent| agent.status_tx.borrow().clone())
    }

    pub(super) fn identity(&self, agent_id: ThreadId) -> Option<ExternalAgentIdentity> {
        self.agent(agent_id).map(|agent| agent.identity.clone())
    }

    pub(super) fn lifecycle_status(&self, agent_id: ThreadId) -> Option<(AgentStatus, u64)> {
        self.agent(agent_id).map(|agent| {
            let runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (agent.status_tx.borrow().clone(), runtime.generation)
        })
    }

    #[cfg(test)]
    pub(super) fn register_for_tests(&self, agent_id: ThreadId, identity: ExternalAgentIdentity) {
        let (status_tx, _) = watch::channel(AgentStatus::PendingInit);
        let (command_tx, _command_rx) = async_channel::unbounded();
        let agent = Arc::new(ExternalAgent {
            identity,
            command_tx,
            runtime: Mutex::new(ExternalAgentRuntime::default()),
            status_tx,
        });
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(agent_id, agent);
    }

    #[cfg(test)]
    pub(super) fn set_status_for_tests(&self, agent_id: ThreadId, status: AgentStatus) {
        if let Some(agent) = self.agent(agent_id) {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(status, AgentStatus::Running) {
                runtime.generation = runtime.generation.wrapping_add(1);
            }
            agent.status_tx.send_replace(status);
        }
    }

    #[cfg(test)]
    pub(super) fn begin_turn_for_tests(&self, agent_id: ThreadId) {
        if let Some(agent) = self.agent(agent_id) {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime.running = true;
            runtime.generation = runtime.generation.wrapping_add(1);
            agent.status_tx.send_replace(AgentStatus::Running);
        }
    }

    #[cfg(test)]
    pub(super) fn finish_turn_for_tests(&self, agent_id: ThreadId, status: AgentStatus) {
        if let Some(agent) = self.agent(agent_id) {
            let generation = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .generation;
            finish_turn(agent, generation, status);
        }
    }

    pub(super) fn take_ready_pending_start(
        &self,
        agent_id: ThreadId,
    ) -> Option<PendingGenerationStart> {
        let agent = self.agent(agent_id)?;
        let turn = {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            take_ready_queued_turn(&mut runtime)
        };
        turn.map(|(prompt, generation, on_started)| PendingGenerationStart {
            agent,
            prompt,
            generation,
            on_started,
            started: false,
        })
    }

    #[cfg(test)]
    pub(super) fn queued_message_contents_for_tests(&self, agent_id: ThreadId) -> Vec<String> {
        self.agent(agent_id)
            .map(|agent| {
                agent
                    .runtime
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .queued_messages
                    .iter()
                    .map(|message| message.content.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(super) fn subscribe(&self, agent_id: ThreadId) -> Option<watch::Receiver<AgentStatus>> {
        self.agent(agent_id)
            .map(|agent| agent.status_tx.subscribe())
    }

    pub(super) fn submit_message(
        &self,
        agent_id: ThreadId,
        content: String,
        trigger_turn: bool,
    ) -> CodexResult<ExternalMessageSubmission> {
        let agent = self
            .agent(agent_id)
            .ok_or(CodexErr::ThreadNotFound(agent_id))?;
        let submission_id = Uuid::now_v7().to_string();
        let action = {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !trigger_turn {
                runtime.queued_messages.push_back(QueuedMessage {
                    content,
                    trigger_turn,
                    submission_id: None,
                    ready_to_start: true,
                    on_started: None,
                });
                ExternalMessageSubmissionAction::QueueOnly
            } else if runtime.running
                || runtime
                    .queued_messages
                    .iter()
                    .any(|message| message.trigger_turn)
            {
                runtime.queued_messages.push_back(QueuedMessage {
                    content,
                    trigger_turn,
                    submission_id: Some(submission_id.clone()),
                    ready_to_start: false,
                    on_started: None,
                });
                ExternalMessageSubmissionAction::ReleaseQueuedTurn {
                    submission_id: submission_id.clone(),
                }
            } else {
                runtime.running = true;
                runtime.generation = runtime.generation.wrapping_add(1);
                ExternalMessageSubmissionAction::StartTurn {
                    queued_messages: runtime.queued_messages.drain(..).collect(),
                    content,
                    generation: runtime.generation,
                }
            }
        };
        Ok(ExternalMessageSubmission {
            submission_id,
            agent,
            action: Some(action),
        })
    }

    pub(super) fn interrupt(&self, agent_id: ThreadId) -> CodexResult<String> {
        let agent = self
            .agent(agent_id)
            .ok_or(CodexErr::ThreadNotFound(agent_id))?;
        let active_task = {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            runtime.running = false;
            runtime.generation = runtime.generation.wrapping_add(1);
            runtime.queued_messages.clear();
            agent.status_tx.send_replace(AgentStatus::Interrupted);
            runtime.active_task.take()
        };
        let _ = agent.command_tx.try_send(AcpCommand::Cancel);
        if let Some(task) = active_task {
            task.abort();
        }
        Ok(Uuid::now_v7().to_string())
    }

    fn agent(&self, agent_id: ThreadId) -> Option<Arc<ExternalAgent>> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&agent_id)
            .cloned()
    }
}

fn prepend_queued_messages(queue: &mut VecDeque<QueuedMessage>, content: String) -> String {
    let mut messages = queue
        .drain(..)
        .map(|message| message.content)
        .collect::<Vec<_>>();
    messages.push(content);
    messages.join("\n\n")
}

fn take_ready_queued_turn(
    runtime: &mut ExternalAgentRuntime,
) -> Option<(String, u64, Option<GenerationStartHook>)> {
    if runtime.running
        || !runtime
            .queued_messages
            .iter()
            .any(|message| message.trigger_turn)
        || runtime
            .queued_messages
            .iter()
            .any(|message| message.trigger_turn && !message.ready_to_start)
    {
        return None;
    }
    runtime.running = true;
    runtime.generation = runtime.generation.wrapping_add(1);
    let mut on_started = None;
    let prompt = runtime
        .queued_messages
        .drain(..)
        .map(|message| {
            if on_started.is_none() {
                on_started = message.on_started;
            }
            message.content
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    Some((prompt, runtime.generation, on_started))
}

fn release_queued_turn(
    agent: Arc<ExternalAgent>,
    submission_id: &str,
) -> Option<PendingGenerationStart> {
    let turn = {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(message) = runtime
            .queued_messages
            .iter_mut()
            .find(|message| message.submission_id.as_deref() == Some(submission_id))
        {
            message.ready_to_start = true;
        }
        take_ready_queued_turn(&mut runtime)
    };
    turn.map(|(prompt, generation, on_started)| PendingGenerationStart {
        agent,
        prompt,
        generation,
        on_started,
        started: false,
    })
}

fn rollback_queued_turn(agent: Arc<ExternalAgent>, submission_id: &str) {
    let pending = {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(position) = runtime
            .queued_messages
            .iter()
            .position(|message| message.submission_id.as_deref() == Some(submission_id))
        {
            runtime.queued_messages.remove(position);
        }
        take_ready_queued_turn(&mut runtime).map(|(prompt, generation, on_started)| {
            PendingGenerationStart {
                agent: Arc::clone(&agent),
                prompt,
                generation,
                on_started,
                started: false,
            }
        })
    };
    if let Some(pending) = pending {
        tokio::spawn(async move {
            pending.start().await;
        });
    }
}

fn rollback_reserved_turn(
    agent: Arc<ExternalAgent>,
    mut queued_messages: VecDeque<QueuedMessage>,
    generation: u64,
) {
    let turn = {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if runtime.running && runtime.generation == generation && runtime.active_task.is_none() {
            runtime.running = false;
            queued_messages.append(&mut runtime.queued_messages);
            runtime.queued_messages = queued_messages;
        }
        take_ready_queued_turn(&mut runtime).map(|(prompt, generation, on_started)| {
            PendingGenerationStart {
                agent: Arc::clone(&agent),
                prompt,
                generation,
                on_started,
                started: false,
            }
        })
    };
    if let Some(pending) = turn {
        tokio::spawn(async move {
            pending.start().await;
        });
    }
}

fn start_turn(agent: Arc<ExternalAgent>, prompt: String, generation: u64) {
    {
        let runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !runtime.running || runtime.generation != generation {
            return;
        }
        agent.status_tx.send_replace(AgentStatus::Running);
    }
    let (response_tx, response_rx) = oneshot::channel();
    let task_agent = Arc::clone(&agent);
    let task = match agent.command_tx.try_send(AcpCommand::Prompt {
        prompt,
        response: response_tx,
    }) {
        Ok(()) => tokio::spawn(async move {
            let status = response_rx.await.unwrap_or_else(|_| {
                AgentStatus::Errored(
                    "ACP harness stopped before completing the prompt turn".to_string(),
                )
            });
            finish_turn(task_agent, generation, status);
        }),
        Err(error) => tokio::spawn(async move {
            finish_turn(
                task_agent,
                generation,
                AgentStatus::Errored(format!("ACP harness is not available: {error}")),
            );
        }),
    };
    let mut runtime = agent
        .runtime
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if runtime.running && runtime.generation == generation {
        runtime.active_task = Some(task.abort_handle());
    }
}

fn finish_turn(agent: Arc<ExternalAgent>, generation: u64, status: AgentStatus) {
    let mut runtime = agent
        .runtime
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if runtime.generation != generation {
        return;
    }
    runtime.running = false;
    runtime.active_task = None;
    // 次generationを始める前に、このgenerationの終端を先に公開する。
    // Stop が先に届き、次の Start は次generationのものになる。
    agent.status_tx.send_replace(status);
}

async fn run_acp_agent(
    backend: ResolvedExternalAgentBackend,
    cwd: std::path::PathBuf,
    env: HashMap<String, String>,
    command_rx: async_channel::Receiver<AcpCommand>,
) -> Result<(), String> {
    let output = Arc::new(Mutex::new(String::new()));
    let notification_output = Arc::clone(&output);
    let harness_name = backend.harness.clone();
    let error_harness_name = harness_name.clone();
    let agent = AcpAgent::new(
        AcpAgentConfig::new(&backend.command)
            .args(backend.args.clone())
            .envs(env),
    );
    agent_client_protocol::Client
        .builder()
        .name("Codex ACP subagent host")
        .on_receive_notification(
            async move |notification: SessionNotification, _connection| {
                append_agent_text(&notification, &notification_output);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: RequestPermissionRequest, responder, _connection| {
                let outcome = request
                    .options
                    .iter()
                    .find(|option| option.kind == PermissionOptionKind::AllowOnce)
                    .map(|option| {
                        RequestPermissionOutcome::Selected(SelectedPermissionOutcome::new(
                            option.option_id.clone(),
                        ))
                    })
                    .unwrap_or(RequestPermissionOutcome::Cancelled);
                responder.respond(RequestPermissionResponse::new(outcome))
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(agent, async move |connection: ConnectionTo<Agent>| {
            connection
                .send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;
            let session = connection
                .send_request(NewSessionRequest::new(&cwd))
                .block_task()
                .await?;
            run_acp_command_loop(connection, session.session_id, command_rx, output).await
        })
        .await
        .map_err(|error| format!("ACP harness `{error_harness_name}` failed: {error}"))
}

async fn run_acp_command_loop(
    connection: ConnectionTo<Agent>,
    session_id: agent_client_protocol::schema::v1::SessionId,
    command_rx: async_channel::Receiver<AcpCommand>,
    output: Arc<Mutex<String>>,
) -> Result<(), agent_client_protocol::Error> {
    while let Ok(command) = command_rx.recv().await {
        match command {
            AcpCommand::Prompt { prompt, response } => {
                output
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
                let mut prompt_request = std::pin::pin!(
                    connection
                        .send_request(PromptRequest::new(
                            session_id.clone(),
                            vec![ContentBlock::Text(
                                agent_client_protocol::schema::v1::TextContent::new(prompt),
                            )],
                        ))
                        .block_task()
                );
                loop {
                    tokio::select! {
                        result = &mut prompt_request => {
                            let status = match result {
                                Ok(prompt_response) => prompt_status(
                                    prompt_response.stop_reason,
                                    take_output(&output),
                                ),
                                Err(error) => AgentStatus::Errored(format!(
                                    "ACP prompt failed: {error}"
                                )),
                            };
                            let _ = response.send(status);
                            break;
                        }
                        command = command_rx.recv() => {
                            match command {
                                Ok(AcpCommand::Cancel) => {
                                    connection.send_notification(
                                        CancelNotification::new(session_id.clone()),
                                    )?;
                                    let result = timeout(
                                        ACP_CANCEL_GRACE_PERIOD,
                                        &mut prompt_request,
                                    )
                                    .await;
                                    let status = match result {
                                        Ok(Ok(prompt_response)) => prompt_status(
                                            prompt_response.stop_reason,
                                            take_output(&output),
                                        ),
                                        Ok(Err(error)) => AgentStatus::Errored(format!(
                                            "ACP prompt cancellation failed: {error}"
                                        )),
                                        Err(_) => {
                                            let message =
                                                "ACP harness did not stop after cancellation";
                                            let _ = response.send(AgentStatus::Errored(
                                                message.to_string(),
                                            ));
                                            return Err(
                                                agent_client_protocol::Error::internal_error()
                                                    .data(message),
                                            );
                                        }
                                    };
                                    let _ = response.send(status);
                                    break;
                                }
                                Ok(AcpCommand::Shutdown) | Err(_) => {
                                    connection.send_notification(
                                        CancelNotification::new(session_id.clone()),
                                    )?;
                                    return Ok(());
                                }
                                Ok(AcpCommand::Prompt { response, .. }) => {
                                    let _ = response.send(AgentStatus::Errored(
                                        "ACP session received overlapping prompt turns".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            AcpCommand::Cancel => {}
            AcpCommand::Shutdown => return Ok(()),
        }
    }
    Ok(())
}

fn append_agent_text(notification: &SessionNotification, output: &Arc<Mutex<String>>) {
    if let SessionUpdate::AgentMessageChunk(ContentChunk {
        content: ContentBlock::Text(text),
        ..
    }) = &notification.update
    {
        output
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_str(&text.text);
    }
}

fn take_output(output: &Arc<Mutex<String>>) -> String {
    let mut output = output
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut *output)
}

fn prompt_status(stop_reason: StopReason, output: String) -> AgentStatus {
    let output = (!output.is_empty())
        .then(|| truncate_text(&output, TruncationPolicy::Tokens(MAX_RESULT_TOKENS)));
    match stop_reason {
        StopReason::EndTurn => AgentStatus::Completed(output),
        StopReason::Cancelled => AgentStatus::Interrupted,
        StopReason::MaxTokens => AgentStatus::Errored(with_output(
            "ACP agent reached its token limit",
            output.as_deref(),
        )),
        StopReason::MaxTurnRequests => AgentStatus::Errored(with_output(
            "ACP agent reached its turn request limit",
            output.as_deref(),
        )),
        StopReason::Refusal => AgentStatus::Errored(with_output(
            "ACP agent refused the prompt",
            output.as_deref(),
        )),
        _ => AgentStatus::Errored(with_output(
            "ACP agent returned an unsupported stop reason",
            output.as_deref(),
        )),
    }
}

fn with_output(message: &str, output: Option<&str>) -> String {
    match output {
        Some(output) if !output.is_empty() => format!("{message}: {output}"),
        _ => message.to_string(),
    }
}

#[cfg(test)]
#[path = "external_tests.rs"]
mod tests;
