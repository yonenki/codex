use super::AgentControl;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::oneshot;

#[cfg(test)]
use std::sync::Mutex;

/// Native または ACP の実行体ができた直後から start 成功までの Team bind 所有権。
/// Drop 時は永続 attach を reconcile し、未commitなら実行体の cleanup のみ、commit 済みなら interrupted を一度だけ記録する。
pub(super) struct AttachToStartOwner {
    control: AgentControl,
    agent_thread_id: ThreadId,
    cleanup_external: bool,
    committed: Arc<AtomicBool>,
    disarmed: Arc<AtomicBool>,
    wakeup: Option<oneshot::Sender<()>>,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct AttachToStartTestControl {
    fail_initial_delivery: AtomicBool,
    hold: Mutex<Option<oneshot::Receiver<()>>>,
    entered: Mutex<Option<oneshot::Sender<()>>>,
}

impl AttachToStartOwner {
    fn arm(control: AgentControl, agent_thread_id: ThreadId, cleanup_external: bool) -> Self {
        let committed = Arc::new(AtomicBool::new(false));
        let disarmed = Arc::new(AtomicBool::new(false));
        let (wakeup, parked) = oneshot::channel();
        let control_task = control.clone();
        let committed_task = Arc::clone(&committed);
        let disarmed_task = Arc::clone(&disarmed);
        tokio::spawn(async move {
            let _ = parked.await;
            if disarmed_task.load(Ordering::SeqCst) {
                return;
            }
            control_task
                .settle_dropped_bind_attempt(
                    agent_thread_id,
                    cleanup_external,
                    committed_task.load(Ordering::SeqCst),
                )
                .await;
        });
        Self {
            control,
            agent_thread_id,
            cleanup_external,
            committed,
            disarmed,
            wakeup: Some(wakeup),
        }
    }

    /// persist callback から呼ぶ。以後の Drop は interrupted へ収束する。
    pub(super) fn on_persisted(&self) -> impl FnOnce(&codex_team_runtime::TeamAgentBinding) {
        let committed = Arc::clone(&self.committed);
        move |_| {
            committed.store(true, Ordering::SeqCst);
        }
    }

    pub(super) fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub(super) fn disarm(mut self) {
        self.release(true);
    }

    pub(super) async fn abandon_uncommitted(mut self) {
        self.control
            .cleanup_bind_attempt_resources(self.agent_thread_id, self.cleanup_external)
            .await;
        self.release(true);
    }

    pub(super) async fn fail_errored(mut self) {
        self.control
            .cleanup_bind_attempt_resources(self.agent_thread_id, self.cleanup_external)
            .await;
        let _ = self
            .control
            .team()
            .record_agent_terminal(&self.agent_thread_id.to_string(), "errored")
            .await;
        self.release(true);
    }

    fn release(&mut self, disarmed: bool) {
        if disarmed {
            self.disarmed.store(true, Ordering::SeqCst);
        }
        let _ = self.wakeup.take();
    }
}

impl Drop for AttachToStartOwner {
    fn drop(&mut self) {
        self.release(false);
    }
}

pub(super) async fn settle_attach_to_start<T>(
    owner: Option<AttachToStartOwner>,
    result: CodexResult<T>,
) -> CodexResult<T> {
    match owner {
        Some(owner) => match result {
            Ok(value) => {
                owner.disarm();
                Ok(value)
            }
            Err(err) => {
                owner.fail_errored().await;
                Err(err)
            }
        },
        None => result,
    }
}

pub(super) async fn abort_attach_to_start(
    owner: Option<AttachToStartOwner>,
    error: CodexErr,
) -> CodexErr {
    match settle_attach_to_start::<()>(owner, Err(error)).await {
        Err(error) => error,
        Ok(_) => CodexErr::Fatal("attach-to-start failure cannot succeed".to_string()),
    }
}

impl AgentControl {
    pub(super) fn arm_attach_to_start(
        &self,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
    ) -> AttachToStartOwner {
        AttachToStartOwner::arm(self.clone(), agent_thread_id, cleanup_external)
    }

    async fn cleanup_bind_attempt_resources(
        &self,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
    ) {
        if cleanup_external {
            self.external_agents.remove(agent_thread_id);
        } else {
            let _ = self.shutdown_live_agent(agent_thread_id).await;
        }
    }

    async fn settle_dropped_bind_attempt(
        &self,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
        committed: bool,
    ) {
        self.cleanup_bind_attempt_resources(agent_thread_id, cleanup_external)
            .await;
        let attached = if committed {
            true
        } else {
            self.team()
                .reconcile_agent_attach(&agent_thread_id.to_string())
                .await
                .unwrap_or(false)
        };
        if attached {
            let _ = self
                .team()
                .record_agent_terminal(&agent_thread_id.to_string(), "interrupted")
                .await;
        }
    }

    #[cfg(test)]
    pub(crate) fn fail_next_spawn_initial_delivery(&self) {
        self.attach_to_start_test
            .fail_initial_delivery
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn hold_next_spawn_after_attach(
        &self,
    ) -> (oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (release_tx, release_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        *self
            .attach_to_start_test
            .hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(release_rx);
        *self
            .attach_to_start_test
            .entered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(entered_tx);
        (release_tx, entered_rx)
    }

    #[cfg(test)]
    pub(super) async fn apply_attach_to_start_test_probe(&self) -> CodexResult<()> {
        let hold = self
            .attach_to_start_test
            .hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let entered = self
            .attach_to_start_test
            .entered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        if let Some(hold) = hold {
            let _ = hold.await;
        }
        if self
            .attach_to_start_test
            .fail_initial_delivery
            .swap(false, Ordering::SeqCst)
        {
            return Err(CodexErr::InvalidRequest(
                "forced spawn initial delivery failure".to_string(),
            ));
        }
        Ok(())
    }
}
