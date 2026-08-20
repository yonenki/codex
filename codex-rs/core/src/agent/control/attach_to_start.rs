use super::AgentControl;
use codex_protocol::ThreadId;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_team_runtime::AgentTerminalStatus;
use codex_team_runtime::BindAttemptHandle;
use codex_team_runtime::BindAttemptOutcome;
use codex_team_runtime::RuntimeProducerPermit;
use codex_team_runtime::TerminalPersistenceOutcome;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;
use tokio::sync::oneshot;

#[cfg(test)]
use std::sync::Mutex as StdMutex;

/// Native または ACP の実行体ができた直後から start 成功までの Team bind 所有権。
/// Drop 時は owned bind の settle を待ち、未commitなら実行体の cleanup のみ、commit 済みなら interrupted を一度だけ記録する。
pub(super) struct AttachToStartOwner {
    control: AgentControl,
    agent_thread_id: ThreadId,
    cleanup_external: bool,
    committed: Arc<AtomicBool>,
    disarmed: Arc<AtomicBool>,
    flight: Arc<Mutex<Option<BindFlight>>>,
    producer: Arc<RuntimeProducerPermit>,
    wakeup: Option<oneshot::Sender<()>>,
}

struct BindFlight {
    cancel: Option<oneshot::Sender<()>>,
    handle: BindAttemptHandle,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct AttachToStartTestControl {
    fail_initial_delivery: AtomicBool,
    fail_native_shutdown_before_send: AtomicBool,
    hold: StdMutex<Option<oneshot::Receiver<()>>>,
    entered: StdMutex<Option<oneshot::Sender<()>>>,
    cleanup_hold: StdMutex<Option<oneshot::Receiver<()>>>,
    cleanup_entered: StdMutex<Option<oneshot::Sender<()>>>,
}

#[derive(Debug)]
enum TerminalCleanupOutcome {
    NotRequired,
    Persisted,
    RetryOwned { first_error: String },
}

#[derive(Debug)]
struct AttachCleanupOutcome {
    resource_error: Option<String>,
    terminal: TerminalCleanupOutcome,
}

impl AttachCleanupOutcome {
    fn observe(self, agent_thread_id: ThreadId) {
        if self.resource_error.is_none()
            && matches!(
                self.terminal,
                TerminalCleanupOutcome::NotRequired | TerminalCleanupOutcome::Persisted
            )
        {
            return;
        }
        let terminal_error = match &self.terminal {
            TerminalCleanupOutcome::RetryOwned { first_error } => Some(first_error.as_str()),
            TerminalCleanupOutcome::NotRequired | TerminalCleanupOutcome::Persisted => None,
        };
        tracing::warn!(
            %agent_thread_id,
            resource_error = ?self.resource_error,
            terminal = ?self.terminal,
            terminal_error,
            "attach-to-start cleanup completed with a recoverable failure"
        );
    }
}

impl AttachToStartOwner {
    fn arm(
        control: AgentControl,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
        producer: RuntimeProducerPermit,
    ) -> Self {
        let committed = Arc::new(AtomicBool::new(false));
        let disarmed = Arc::new(AtomicBool::new(false));
        let flight = Arc::new(Mutex::new(None));
        let (wakeup, parked) = oneshot::channel();
        let control_task = control.clone();
        let committed_task = Arc::clone(&committed);
        let disarmed_task = Arc::clone(&disarmed);
        let flight_task = Arc::clone(&flight);
        let producer = Arc::new(producer);
        let producer_task = Arc::clone(&producer);
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
                    flight_task.lock().await.take(),
                    &producer_task,
                )
                .await;
        });
        Self {
            control,
            agent_thread_id,
            cleanup_external,
            committed,
            disarmed,
            flight,
            producer,
            wakeup: Some(wakeup),
        }
    }

    /// persist callback から呼ぶ。以後の Drop は interrupted へ収束する。
    pub(super) fn on_persisted(
        &self,
    ) -> impl FnOnce(&codex_team_runtime::TeamAgentBinding) + Send + 'static + use<> {
        let committed = Arc::clone(&self.committed);
        move |_| {
            committed.store(true, Ordering::SeqCst);
        }
    }

    pub(super) fn is_committed(&self) -> bool {
        self.committed.load(Ordering::SeqCst)
    }

    pub(super) async fn bind_pending(
        self,
        pending: codex_team_runtime::PendingTeamBinding,
    ) -> CodexResult<Self> {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let on_persisted = self.on_persisted();
        let handle: BindAttemptHandle = self.control.team_handle().spawn_bind_attempt(
            self.agent_thread_id.to_string(),
            pending,
            on_persisted,
            cancel_rx,
        );
        {
            let mut flight = self.flight.lock().await;
            *flight = Some(BindFlight {
                cancel: Some(cancel_tx),
                handle: handle.clone(),
            });
        }
        match handle.wait().await {
            BindAttemptOutcome::Attached(_) => Ok(self),
            BindAttemptOutcome::Uncommitted => {
                self.abandon_uncommitted().await;
                Err(CodexErr::InvalidRequest(
                    "team bind cancelled before persist".to_string(),
                ))
            }
            BindAttemptOutcome::Failed { error, committed } => {
                let mapped = CodexErr::InvalidRequest(error.to_string());
                if committed || self.is_committed() {
                    Err(abort_attach_to_start(Some(self), mapped).await)
                } else {
                    self.abandon_uncommitted().await;
                    Err(mapped)
                }
            }
        }
    }

    pub(super) fn disarm(mut self) {
        self.release(true);
    }

    pub(super) async fn abandon_uncommitted(mut self) {
        let resource_error = self
            .control
            .cleanup_bind_attempt_resources(self.agent_thread_id, self.cleanup_external)
            .await;
        AttachCleanupOutcome {
            resource_error,
            terminal: TerminalCleanupOutcome::NotRequired,
        }
        .observe(self.agent_thread_id);
        self.release(true);
    }

    pub(super) async fn fail_errored(mut self) {
        let resource_error = self
            .control
            .cleanup_bind_attempt_resources(self.agent_thread_id, self.cleanup_external)
            .await;
        let terminal = self
            .control
            .record_attach_terminal(
                &self.producer,
                self.agent_thread_id,
                AgentTerminalStatus::Errored,
            )
            .await;
        AttachCleanupOutcome {
            resource_error,
            terminal,
        }
        .observe(self.agent_thread_id);
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
        producer: RuntimeProducerPermit,
    ) -> AttachToStartOwner {
        AttachToStartOwner::arm(self.clone(), agent_thread_id, cleanup_external, producer)
    }

    async fn cleanup_bind_attempt_resources(
        &self,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
    ) -> Option<String> {
        if cleanup_external {
            self.external_agents.remove(agent_thread_id);
            self.forget_v2_residency(agent_thread_id);
            self.state.release_spawned_thread(agent_thread_id);
            None
        } else {
            #[cfg(test)]
            self.apply_cleanup_before_registry_test_probe().await;
            self.shutdown_live_agent(agent_thread_id)
                .await
                .err()
                .map(|error| error.to_string())
        }
    }

    async fn record_attach_terminal(
        &self,
        producer: &RuntimeProducerPermit,
        agent_thread_id: ThreadId,
        status: AgentTerminalStatus,
    ) -> TerminalCleanupOutcome {
        match self
            .team_handle()
            .record_agent_terminal_managed(producer, &agent_thread_id.to_string(), status)
            .await
        {
            Ok(
                TerminalPersistenceOutcome::Persisted | TerminalPersistenceOutcome::AlreadyTerminal,
            ) => TerminalCleanupOutcome::Persisted,
            Ok(TerminalPersistenceOutcome::RetryPending { first_error }) => {
                TerminalCleanupOutcome::RetryOwned { first_error }
            }
            Err(error) => TerminalCleanupOutcome::RetryOwned {
                first_error: error.to_string(),
            },
        }
    }

    async fn settle_dropped_bind_attempt(
        &self,
        agent_thread_id: ThreadId,
        cleanup_external: bool,
        committed: bool,
        flight: Option<BindFlight>,
        producer: &RuntimeProducerPermit,
    ) {
        let outcome = if let Some(mut flight) = flight {
            if let Some(cancel) = flight.cancel.take() {
                let _ = cancel.send(());
            }
            Some(flight.handle.wait().await)
        } else {
            None
        };
        let resource_error = self
            .cleanup_bind_attempt_resources(agent_thread_id, cleanup_external)
            .await;
        let attached = match &outcome {
            None => committed,
            Some(BindAttemptOutcome::Uncommitted) => false,
            Some(BindAttemptOutcome::Attached(_)) => true,
            Some(BindAttemptOutcome::Failed {
                committed: persist_committed,
                ..
            }) => committed || *persist_committed,
        };
        if attached {
            let terminal = self
                .record_attach_terminal(producer, agent_thread_id, AgentTerminalStatus::Interrupted)
                .await;
            AttachCleanupOutcome {
                resource_error,
                terminal,
            }
            .observe(agent_thread_id);
            return;
        }
        if outcome.is_some() && !matches!(outcome, Some(BindAttemptOutcome::Uncommitted)) {
            // settle 報告が失敗でも、正規の terminal mutation で active なら一度記録する。
            let terminal = self
                .record_attach_terminal(producer, agent_thread_id, AgentTerminalStatus::Interrupted)
                .await;
            AttachCleanupOutcome {
                resource_error,
                terminal,
            }
            .observe(agent_thread_id);
            return;
        }
        AttachCleanupOutcome {
            resource_error,
            terminal: TerminalCleanupOutcome::NotRequired,
        }
        .observe(agent_thread_id);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_spawn_initial_delivery(&self) {
        self.attach_to_start_test
            .fail_initial_delivery
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn fail_next_native_shutdown_before_send(&self) {
        self.attach_to_start_test
            .fail_native_shutdown_before_send
            .store(true, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(super) fn take_native_shutdown_failure_probe(&self) -> bool {
        self.attach_to_start_test
            .fail_native_shutdown_before_send
            .swap(false, Ordering::SeqCst)
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
    pub(crate) fn hold_next_cleanup_before_registry(
        &self,
    ) -> (oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (release_tx, release_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        *self
            .attach_to_start_test
            .cleanup_hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(release_rx);
        *self
            .attach_to_start_test
            .cleanup_entered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(entered_tx);
        (release_tx, entered_rx)
    }

    #[cfg(test)]
    async fn apply_cleanup_before_registry_test_probe(&self) {
        let hold = self
            .attach_to_start_test
            .cleanup_hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let entered = self
            .attach_to_start_test
            .cleanup_entered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        if let Some(hold) = hold {
            let _ = hold.await;
        }
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
