use std::sync::Arc;

use codex_hooks::SessionStartRequest;
use codex_hooks::StartHookTarget;
use codex_hooks::StopHookTarget;
use codex_hooks::StopRequest;
use codex_hooks::SubagentBackendIdentity;
use codex_protocol::ThreadId;

use crate::hook_runtime::emit_hook_completed_events;
use crate::hook_runtime::emit_hook_started_events;
use crate::hook_runtime::hook_permission_mode;
use crate::session::session::Session;
use crate::session::turn_context::TurnContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExternalSubagentHookIdentity {
    pub agent_id: ThreadId,
    pub agent_type: String,
    pub harness: String,
    pub model: Option<String>,
    pub metadata: Option<std::collections::BTreeMap<String, String>>,
}

impl ExternalSubagentHookIdentity {
    fn backend(&self) -> SubagentBackendIdentity {
        SubagentBackendIdentity {
            harness: self.harness.clone(),
            model: self.model.clone(),
        }
    }
}

pub(crate) async fn run_external_subagent_start_hook(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    identity: ExternalSubagentHookIdentity,
) {
    let backend = identity.backend();
    let request = SessionStartRequest {
        session_id: session.session_id().into(),
        #[allow(deprecated)]
        cwd: turn.cwd.clone(),
        transcript_path: session.hook_transcript_path().await,
        model: turn.model_info.slug.clone(),
        permission_mode: hook_permission_mode(turn),
        target: StartHookTarget::SubagentStart {
            turn_id: turn.sub_id.clone(),
            agent_id: identity.agent_id.to_string(),
            agent_type: identity.agent_type,
            backend: Some(backend),
            metadata: identity.metadata,
        },
    };
    let hooks = session.hooks();
    emit_hook_started_events(session, turn, hooks.preview_session_start(&request)).await;
    let mut outcome = hooks
        .run_session_start(request, Some(turn.sub_id.clone()))
        .await;
    emit_hook_completed_events(session, turn, std::mem::take(&mut outcome.hook_events)).await;
}

pub(crate) async fn run_external_subagent_stop_hook(
    session: &Arc<Session>,
    turn: &Arc<TurnContext>,
    identity: ExternalSubagentHookIdentity,
) {
    let backend = identity.backend();
    let request = StopRequest {
        session_id: session.session_id().into(),
        turn_id: turn.sub_id.clone(),
        #[allow(deprecated)]
        cwd: turn.cwd.clone(),
        transcript_path: session.hook_transcript_path().await,
        model: turn.model_info.slug.clone(),
        permission_mode: hook_permission_mode(turn),
        stop_hook_active: false,
        last_assistant_message: None,
        target: StopHookTarget::SubagentStop {
            agent_id: identity.agent_id.to_string(),
            agent_type: identity.agent_type,
            agent_transcript_path: None,
            backend: Some(backend),
        },
    };
    let hooks = session.hooks();
    emit_hook_started_events(session, turn, hooks.preview_stop(&request)).await;
    let mut outcome = hooks.run_stop(request).await;
    emit_hook_completed_events(session, turn, std::mem::take(&mut outcome.hook_events)).await;
}
