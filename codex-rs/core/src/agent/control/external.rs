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
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::Duration;
use tokio::time::timeout;

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

    pub(super) fn subscribe(&self, agent_id: ThreadId) -> Option<watch::Receiver<AgentStatus>> {
        self.agent(agent_id)
            .map(|agent| agent.status_tx.subscribe())
    }

    pub(super) fn submit_message(
        &self,
        agent_id: ThreadId,
        content: String,
        trigger_turn: bool,
    ) -> CodexResult<(String, bool)> {
        let agent = self
            .agent(agent_id)
            .ok_or(CodexErr::ThreadNotFound(agent_id))?;
        let submission_id = Uuid::now_v7().to_string();
        let turn = {
            let mut runtime = agent
                .runtime
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if runtime.running || !trigger_turn {
                runtime.queued_messages.push_back(QueuedMessage {
                    content,
                    trigger_turn,
                });
                None
            } else {
                runtime.running = true;
                runtime.generation = runtime.generation.wrapping_add(1);
                Some((
                    prepend_queued_messages(&mut runtime.queued_messages, content),
                    runtime.generation,
                ))
            }
        };
        let started_turn = turn.is_some();
        if let Some((prompt, generation)) = turn {
            start_turn(Arc::clone(&agent), prompt, generation);
        }
        Ok((submission_id, started_turn))
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
            runtime.active_task.take()
        };
        let _ = agent.command_tx.try_send(AcpCommand::Cancel);
        if let Some(task) = active_task {
            task.abort();
        }
        agent.status_tx.send_replace(AgentStatus::Interrupted);
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

fn start_turn(agent: Arc<ExternalAgent>, prompt: String, generation: u64) {
    agent.status_tx.send_replace(AgentStatus::Running);
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
    let next_turn = {
        let mut runtime = agent
            .runtime
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if runtime.generation != generation {
            return;
        }
        runtime.running = false;
        runtime.active_task = None;
        let should_start = runtime
            .queued_messages
            .iter()
            .any(|message| message.trigger_turn);
        should_start.then(|| {
            runtime.running = true;
            runtime.generation = runtime.generation.wrapping_add(1);
            (
                runtime
                    .queued_messages
                    .drain(..)
                    .map(|message| message.content)
                    .collect::<Vec<_>>()
                    .join("\n\n"),
                runtime.generation,
            )
        })
    };
    if let Some((prompt, generation)) = next_turn {
        start_turn(agent, prompt, generation);
    } else {
        agent.status_tx.send_replace(status);
    }
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
