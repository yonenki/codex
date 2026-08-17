use super::AgentControl;
use crate::agent::AgentStatus;
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
}

impl TerminalStatusTracker {
    pub(super) fn should_notify(&mut self, status: &AgentStatus) -> bool {
        match terminal_status(status) {
            Some(status) => {
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

    let event = Event {
        id: child_thread_id.to_string(),
        msg: EventMsg::SubAgentTerminal(SubAgentTerminalEvent {
            agent_thread_id: child_thread_id,
            agent_path: Some(agent_path),
            agent_nickname: metadata.agent_nickname,
            agent_role: metadata.agent_role,
            status,
        }),
    };
    parent_thread.send_subagent_terminal_event(event).await;
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
        assert!(tracker.should_notify(&AgentStatus::Interrupted));
        assert!(!tracker.should_notify(&AgentStatus::Interrupted));
        assert!(!tracker.should_notify(&AgentStatus::Running));
        assert!(tracker.should_notify(&AgentStatus::Completed(None)));
        assert!(!tracker.should_notify(&AgentStatus::Completed(None)));
        assert!(!tracker.should_notify(&AgentStatus::Errored("late".to_string())));

        tracker.should_notify(&AgentStatus::Running);
        assert!(tracker.should_notify(&AgentStatus::Errored("late".to_string())));
        assert!(!tracker.should_notify(&AgentStatus::Errored("late".to_string())));
    }
}
