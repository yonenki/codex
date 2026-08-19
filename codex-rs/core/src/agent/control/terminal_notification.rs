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
    let Some(metadata) = control.state.agent_metadata_for_thread(child_thread_id) else {
        return;
    };
    let Some(agent_path) = metadata.agent_path.clone() else {
        return;
    };
    let Some(parent_agent_path) = agent_path
        .as_str()
        .rsplit_once('/')
        .and_then(|(parent, _)| AgentPath::try_from(parent).ok())
    else {
        return;
    };
    let Some(parent_thread_id) = control.state.agent_id_for_path(&parent_agent_path) else {
        return;
    };
    let Ok(state) = control.upgrade() else {
        return;
    };
    let Ok(parent_thread) = state.get_thread(parent_thread_id).await else {
        return;
    };
    let external_identity = control.external_agents.identity(child_thread_id);
    let is_external = external_identity.is_some();
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

    let event = Event {
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
    };
    parent_thread.send_subagent_terminal_event(event).await;
    if let Some(identity) = hook_identity {
        let turn = parent_thread.session.new_default_turn().await;
        Box::pin(run_external_subagent_stop_hook(
            &parent_thread.session,
            &turn,
            identity,
        ))
        .await;
    }
    if is_external {
        Box::pin(control.promote_ready_external_generation(child_thread_id)).await;
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
