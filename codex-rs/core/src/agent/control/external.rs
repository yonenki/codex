use super::*;
use crate::agent::role::ExternalAgentBackend;
use codex_utils_output_truncation::TruncationPolicy;
use codex_utils_output_truncation::truncate_text;
use serde_json::Value;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::process::Stdio;
use std::sync::Mutex;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::watch;
use tokio::task::AbortHandle;

const MAX_STDERR_BYTES: usize = 32 * 1024;
const MAX_RESULT_TOKENS: usize = 8_000;

#[derive(Default)]
pub(super) struct ExternalAgentManager {
    agents: Mutex<HashMap<ThreadId, Arc<ExternalAgent>>>,
}

struct ExternalAgent {
    backend: ExternalAgentBackend,
    cwd: std::path::PathBuf,
    env: HashMap<String, String>,
    conversation_id: Mutex<Option<String>>,
    runtime: Mutex<ExternalAgentRuntime>,
    status_tx: watch::Sender<AgentStatus>,
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

impl ExternalAgentManager {
    pub(super) fn contains(&self, agent_id: ThreadId) -> bool {
        self.agent(agent_id).is_some()
    }

    pub(super) fn register(
        &self,
        agent_id: ThreadId,
        backend: ExternalAgentBackend,
        cwd: std::path::PathBuf,
        env: HashMap<String, String>,
    ) -> CodexResult<()> {
        let (status_tx, _) = watch::channel(AgentStatus::PendingInit);
        let agent = Arc::new(ExternalAgent {
            backend,
            cwd,
            env,
            conversation_id: Mutex::new(None),
            runtime: Mutex::new(ExternalAgentRuntime::default()),
            status_tx,
        });
        let mut agents = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if agents.insert(agent_id, agent).is_some() {
            return Err(CodexErr::Fatal(format!(
                "external agent id {agent_id} was registered twice"
            )));
        }
        Ok(())
    }

    pub(super) fn remove(&self, agent_id: ThreadId) {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&agent_id);
    }

    pub(super) fn status(&self, agent_id: ThreadId) -> Option<AgentStatus> {
        self.agent(agent_id)
            .map(|agent| agent.status_tx.borrow().clone())
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
    let task_agent = Arc::clone(&agent);
    let task = tokio::spawn(async move {
        let status = run_antigravity_turn(&task_agent, prompt).await;
        finish_turn(task_agent, generation, status);
    });
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

async fn run_antigravity_turn(agent: &ExternalAgent, prompt: String) -> AgentStatus {
    let conversation_id = agent
        .conversation_id
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let mut command = tokio::process::Command::new(&agent.backend.command);
    command
        .current_dir(&agent.cwd)
        .env_clear()
        .envs(&agent.env)
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .args([
            "--output-format",
            "stream-json",
            "--model",
            agent.backend.model.as_str(),
            "--disable-slash-commands",
            "--dangerously-skip-permissions",
            "--print-timeout",
            "30m",
        ]);
    if let Some(conversation_id) = conversation_id {
        command.args(["--conversation", conversation_id.as_str()]);
    }
    command.args(["--print", prompt.as_str()]);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            return AgentStatus::Errored(format!(
                "failed to start Antigravity CLI `{}`: {error}",
                agent.backend.command
            ));
        }
    };
    let Some(stdout) = child.stdout.take() else {
        return AgentStatus::Errored("Antigravity CLI did not expose stdout".to_string());
    };
    let stderr_task = child
        .stderr
        .take()
        .map(|stderr| tokio::spawn(read_bounded_stderr(stderr)));
    let mut stdout = BufReader::new(stdout).lines();
    let mut result = None;
    loop {
        match stdout.next_line().await {
            Ok(Some(line)) => match parse_stream_event(&line) {
                Ok(StreamEvent::Conversation(id)) => {
                    *agent
                        .conversation_id
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(id);
                }
                Ok(StreamEvent::Result(status)) => result = Some(status),
                Ok(StreamEvent::Ignore) => {}
                Err(error) => return AgentStatus::Errored(error),
            },
            Ok(None) => break,
            Err(error) => {
                return AgentStatus::Errored(format!(
                    "failed to read Antigravity CLI output: {error}"
                ));
            }
        }
    }
    let exit_status = child.wait().await;
    let stderr = match stderr_task {
        Some(task) => task.await.unwrap_or_default(),
        None => String::new(),
    };
    match exit_status {
        Ok(status) if status.success() => result.unwrap_or_else(|| {
            AgentStatus::Errored(with_stderr(
                "Antigravity CLI exited without a result event",
                &stderr,
            ))
        }),
        Ok(status) => AgentStatus::Errored(with_stderr(
            &format!("Antigravity CLI exited with status {status}"),
            &stderr,
        )),
        Err(error) => AgentStatus::Errored(with_stderr(
            &format!("failed to wait for Antigravity CLI: {error}"),
            &stderr,
        )),
    }
}

async fn read_bounded_stderr(stderr: tokio::process::ChildStderr) -> String {
    let mut lines = BufReader::new(stderr).lines();
    let mut retained = VecDeque::<String>::new();
    let mut retained_bytes = 0usize;
    while let Ok(Some(line)) = lines.next_line().await {
        retained_bytes = retained_bytes.saturating_add(line.len() + 1);
        retained.push_back(line);
        while retained_bytes > MAX_STDERR_BYTES {
            let Some(removed) = retained.pop_front() else {
                break;
            };
            retained_bytes = retained_bytes.saturating_sub(removed.len() + 1);
        }
    }
    retained.into_iter().collect::<Vec<_>>().join("\n")
}

fn with_stderr(message: &str, stderr: &str) -> String {
    if stderr.trim().is_empty() {
        message.to_string()
    } else {
        format!("{message}: {}", stderr.trim())
    }
}

enum StreamEvent {
    Conversation(String),
    Result(AgentStatus),
    Ignore,
}

fn parse_stream_event(line: &str) -> Result<StreamEvent, String> {
    let event: Value = serde_json::from_str(line)
        .map_err(|error| format!("invalid Antigravity stream event: {error}"))?;
    match event.get("event").and_then(Value::as_str) {
        Some("init") => event
            .get("conversation_id")
            .and_then(Value::as_str)
            .map(|id| StreamEvent::Conversation(id.to_string()))
            .ok_or_else(|| "Antigravity init event omitted conversation_id".to_string()),
        Some("result") => {
            let result = event
                .get("result")
                .ok_or_else(|| "Antigravity result event omitted result".to_string())?;
            let status = result
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("ERROR");
            let response = result
                .get("response")
                .and_then(Value::as_str)
                .map(|response| {
                    truncate_text(response, TruncationPolicy::Tokens(MAX_RESULT_TOKENS))
                });
            if status == "SUCCESS" {
                Ok(StreamEvent::Result(AgentStatus::Completed(response)))
            } else {
                Ok(StreamEvent::Result(AgentStatus::Errored(
                    response.unwrap_or_else(|| format!("Antigravity result status was {status}")),
                )))
            }
        }
        Some(_) => Ok(StreamEvent::Ignore),
        None => Err("Antigravity stream event omitted event kind".to_string()),
    }
}

#[cfg(test)]
#[path = "external_tests.rs"]
mod tests;
