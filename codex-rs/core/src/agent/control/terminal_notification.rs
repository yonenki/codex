use super::AgentControl;
use crate::agent::AgentStatus;
use crate::agent::role::ACP_ROLE_NAME;
use crate::external_subagent_hooks::ExternalSubagentHookIdentity;
use crate::external_subagent_hooks::run_external_subagent_stop_hook;
use codex_protocol::AgentPath;
use codex_protocol::ThreadId;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::SubAgentTerminalEvent;
use codex_protocol::protocol::SubAgentTerminalStatus;
use codex_team_runtime::AgentTerminalStatus;
use codex_team_runtime::TerminalPersistenceOutcome;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::sync::watch;

fn terminal_status(status: &AgentStatus) -> Option<SubAgentTerminalStatus> {
    match status {
        AgentStatus::Completed(_) => Some(SubAgentTerminalStatus::Completed),
        AgentStatus::Errored(_) => Some(SubAgentTerminalStatus::Errored),
        AgentStatus::Interrupted => Some(SubAgentTerminalStatus::Interrupted),
        AgentStatus::PendingInit
        | AgentStatus::Running
        | AgentStatus::Shutdown
        | AgentStatus::NotFound => None,
    }
}

#[derive(Default)]
pub(super) struct TerminalStatusTracker {
    last_notified: Option<SubAgentTerminalStatus>,
    last_external_generation: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ManagedTerminalNotificationState {
    Pending,
    Persisted,
    Failed,
}

#[derive(Default)]
pub(super) struct ManagedTerminalNotifications {
    states: Mutex<HashMap<ThreadId, ManagedTerminalNotificationEntry>>,
}

struct ManagedTerminalNotificationEntry {
    completion: watch::Receiver<ManagedTerminalNotificationState>,
    notification: Option<ParentTerminalNotification>,
    claimed: bool,
}

struct ParentTerminalNotification {
    parent_thread: Arc<crate::CodexThread>,
    event: Event,
    hook_identity: Option<ExternalSubagentHookIdentity>,
}

impl ParentTerminalNotification {
    async fn deliver(self) {
        self.parent_thread
            .send_subagent_terminal_event(self.event)
            .await;
        if let Some(identity) = self.hook_identity {
            let turn = self.parent_thread.session.new_default_turn().await;
            Box::pin(run_external_subagent_stop_hook(
                &self.parent_thread.session,
                &turn,
                identity,
            ))
            .await;
        }
    }
}

impl TerminalStatusTracker {
    pub(super) fn should_notify(
        &mut self,
        status: &AgentStatus,
        external_generation: Option<u64>,
    ) -> bool {
        match terminal_status(status) {
            Some(status) => {
                if let Some(generation) = external_generation {
                    if self.last_external_generation == Some(generation) {
                        return false;
                    }
                    self.last_external_generation = Some(generation);
                    self.last_notified = Some(status);
                    return true;
                }
                if self.last_notified == Some(status) {
                    false
                } else {
                    self.last_notified = Some(status);
                    true
                }
            }
            None => {
                if matches!(status, AgentStatus::Running) {
                    self.last_notified = None;
                }
                false
            }
        }
    }
}

/// Deliver a transient terminal transition to the direct parent, if the registry can resolve
/// the child path and parent thread. No completion text or error payload is included.
pub(super) async fn maybe_notify_parent_of_terminal_status(
    control: &AgentControl,
    child_thread_id: ThreadId,
    status: &AgentStatus,
) {
    let Some(status) = terminal_status(status) else {
        return;
    };
    let durable_status = match status {
        SubAgentTerminalStatus::Completed => AgentTerminalStatus::Completed,
        SubAgentTerminalStatus::Errored => AgentTerminalStatus::Errored,
        SubAgentTerminalStatus::Interrupted => AgentTerminalStatus::Interrupted,
    };
    let notification = prepare_parent_terminal_notification(control, child_thread_id, status).await;
    if control
        .team
        .binding_snapshot(&child_thread_id.to_string())
        .is_some()
    {
        Box::pin(notify_managed_team_terminal(
            control,
            child_thread_id,
            durable_status,
            notification,
        ))
        .await;
        return;
    }
    if let Some(notification) = notification {
        notification.deliver().await;
    }
}

async fn prepare_parent_terminal_notification(
    control: &AgentControl,
    child_thread_id: ThreadId,
    status: SubAgentTerminalStatus,
) -> Option<ParentTerminalNotification> {
    let metadata = control.state.agent_metadata_for_thread(child_thread_id)?;
    let agent_path = metadata.agent_path.clone()?;
    let parent_agent_path = agent_path
        .as_str()
        .rsplit_once('/')
        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())?;
    let parent_thread_id = control.state.agent_id_for_path(&parent_agent_path)?;
    let Ok(state) = control.upgrade() else {
        return None;
    };
    let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
        return None;
    };
    let external_identity = control.external_agents.identity(child_thread_id);
    let hook_identity = external_identity
        .as_ref()
        .map(|identity| ExternalSubagentHookIdentity {
            agent_id: child_thread_id,
            agent_type: metadata
                .agent_role
                .clone()
                .unwrap_or_else(|| ACP_ROLE_NAME.to_string()),
            harness: identity.harness.clone(),
            model: identity.model.clone(),
            metadata: None,
        });

    Some(ParentTerminalNotification {
        parent_thread,
        event: Event {
            id: child_thread_id.to_string(),
            msg: EventMsg::SubAgentTerminal(SubAgentTerminalEvent {
                agent_thread_id: child_thread_id,
                agent_path: Some(agent_path),
                agent_nickname: metadata.agent_nickname,
                agent_role: metadata.agent_role,
                harness: external_identity
                    .as_ref()
                    .map(|identity| identity.harness.clone()),
                model: external_identity.and_then(|identity| identity.model),
                status,
            }),
        },
        hook_identity,
    })
}

async fn notify_managed_team_terminal(
    control: &AgentControl,
    child_thread_id: ThreadId,
    status: AgentTerminalStatus,
    notification: Option<ParentTerminalNotification>,
) {
    let mut completion = {
        let mut states = control
            .managed_terminal_notifications
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = states.get(&child_thread_id) {
            entry.completion.clone()
        } else {
            let (completed, completion) = watch::channel(ManagedTerminalNotificationState::Pending);
            states.insert(
                child_thread_id,
                ManagedTerminalNotificationEntry {
                    completion: completion.clone(),
                    notification,
                    claimed: false,
                },
            );
            match control.team.begin_runtime_producer() {
                Ok(producer) => {
                    let control = control.clone();
                    tokio::spawn(async move {
                        let agent_thread_id = child_thread_id.to_string();
                        let persisted = match control
                            .team
                            .record_agent_terminal_managed(&producer, &agent_thread_id, status)
                            .await
                        {
                            Ok(
                                TerminalPersistenceOutcome::Persisted
                                | TerminalPersistenceOutcome::AlreadyTerminal,
                            ) => Ok(()),
                            Ok(TerminalPersistenceOutcome::RetryPending { .. }) => {
                                control
                                    .team
                                    .wait_for_agent_terminal_persistence(&agent_thread_id)
                                    .await
                            }
                            Err(error) => Err(error),
                        };
                        match persisted {
                            Ok(()) => {
                                completed.send_replace(ManagedTerminalNotificationState::Persisted);
                            }
                            Err(error) => {
                                tracing::warn!(
                                    %child_thread_id,
                                    %error,
                                    "managed Team terminal persistence ended without parent notification"
                                );
                                completed.send_replace(ManagedTerminalNotificationState::Failed);
                            }
                        }
                    });
                }
                Err(error) => {
                    tracing::warn!(
                        %child_thread_id,
                        %error,
                        "managed Team terminal persistence was rejected before parent notification"
                    );
                    completed.send_replace(ManagedTerminalNotificationState::Failed);
                }
            }
            completion
        }
    };
    loop {
        let state = *completion.borrow();
        match state {
            ManagedTerminalNotificationState::Pending => {}
            ManagedTerminalNotificationState::Persisted => {
                let notification = {
                    let mut states = control
                        .managed_terminal_notifications
                        .states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = states.get_mut(&child_thread_id) else {
                        return;
                    };
                    if entry.claimed {
                        None
                    } else {
                        entry.claimed = true;
                        entry.notification.take()
                    }
                };
                if let Some(notification) = notification {
                    notification.deliver().await;
                }
                return;
            }
            ManagedTerminalNotificationState::Failed => return,
        }
        if completion.changed().await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TerminalStatusTracker;
    use super::terminal_status;
    use crate::agent::AgentStatus;
    use codex_protocol::protocol::SubAgentTerminalStatus;

    #[test]
    fn maps_typed_terminal_status_without_forwarding_bodies() {
        assert_eq!(
            terminal_status(&AgentStatus::Completed(Some("child output".to_string()))),
            Some(SubAgentTerminalStatus::Completed)
        );
        assert_eq!(
            terminal_status(&AgentStatus::Errored("child error".to_string())),
            Some(SubAgentTerminalStatus::Errored)
        );
        assert_eq!(
            terminal_status(&AgentStatus::Interrupted),
            Some(SubAgentTerminalStatus::Interrupted)
        );
        assert_eq!(terminal_status(&AgentStatus::Running), None);
    }

    #[test]
    fn interrupted_running_terminal_transitions_notify_once() {
        let mut tracker = TerminalStatusTracker::default();
        assert!(tracker.should_notify(&AgentStatus::Interrupted, None));
        assert!(!tracker.should_notify(&AgentStatus::Interrupted, None));
        assert!(!tracker.should_notify(&AgentStatus::Running, None));
        assert!(tracker.should_notify(&AgentStatus::Completed(None), None));
        assert!(!tracker.should_notify(&AgentStatus::Completed(None), None));
        assert!(tracker.should_notify(&AgentStatus::Errored("late".to_string()), None));
        assert!(!tracker.should_notify(&AgentStatus::Errored("late".to_string()), None));

        tracker.should_notify(&AgentStatus::Running, None);
        assert!(tracker.should_notify(&AgentStatus::Errored("late".to_string()), None));
        assert!(!tracker.should_notify(&AgentStatus::Errored("late".to_string()), None));
    }

    #[test]
    fn external_generation_distinguishes_terminal_lifecycles_without_running_observation() {
        let mut tracker = TerminalStatusTracker::default();
        assert!(tracker.should_notify(&AgentStatus::Completed(None), Some(1)));
        assert!(!tracker.should_notify(&AgentStatus::Completed(None), Some(1)));
        assert!(tracker.should_notify(&AgentStatus::Completed(None), Some(2)));
        assert!(!tracker.should_notify(&AgentStatus::Completed(None), Some(2)));
    }
}
