use crate::binding::TeamAgentBinding;
use crate::error::TeamRuntimeError;
use crate::error::TeamRuntimeResult;
use crate::event::TeamEvent;
use crate::event::TeamEventKind;
use crate::event::TeamEventPayload;
use crate::ids::NodeRunId;
use crate::state::NodeRun;
use crate::state::TeamLifecycle;
use crate::state::TeamSessionState;

pub fn reduce(state: &mut TeamSessionState, event: &TeamEvent) -> TeamRuntimeResult<()> {
    if event.team_session_id != state.team_session_id {
        return Err(TeamRuntimeError::CrossTeamRef {
            team: state.team_session_id.clone(),
            subject: event.team_session_id.to_string(),
        });
    }
    if event.sequence != state.next_sequence {
        return Err(TeamRuntimeError::invalid(format!(
            "expected sequence {}, got {}",
            state.next_sequence, event.sequence
        )));
    }
    apply_kind(state, event)?;
    state.next_sequence = event.sequence.saturating_add(1);
    if event.kind.bumps_revision() {
        state.revision = state.revision.next();
    }
    Ok(())
}

fn apply_kind(state: &mut TeamSessionState, event: &TeamEvent) -> TeamRuntimeResult<()> {
    match event.kind {
        TeamEventKind::TeamStarted => Ok(()),
        TeamEventKind::TeamCompleted => {
            state.lifecycle = TeamLifecycle::Completed;
            state.waiting_reason = None;
            Ok(())
        }
        TeamEventKind::TeamAborted => {
            state.lifecycle = TeamLifecycle::Aborted;
            state.waiting_reason = None;
            Ok(())
        }
        TeamEventKind::NodeStarted => {
            let node_id = event
                .node_id
                .clone()
                .ok_or_else(|| TeamRuntimeError::invalid("node started event missing node_id"))?;
            let node_run_id = event
                .node_run_id
                .clone()
                .unwrap_or_else(NodeRunId::generate);
            state.current_node_id = node_id.clone();
            state.current_node_run = Some(NodeRun {
                node_run_id,
                node_id,
                attempt: event.attempt.unwrap_or(1),
                started_at: event.occurred_at,
                result: None,
                completed_at: None,
            });
            state.lifecycle = TeamLifecycle::Running;
            Ok(())
        }
        TeamEventKind::NodeCompleted => {
            if let TeamEventPayload::NodeCompleted {
                result,
                candidate_sha,
                evidence_id,
            } = &event.payload
            {
                if let Some(run) = state.current_node_run.as_mut() {
                    run.result = Some(result.clone());
                    run.completed_at = Some(event.occurred_at);
                }
                state.last_result = Some(result.clone());
                // candidate SHA は payload の明示欄だけを正本にする。
                if let Some(sha) = candidate_sha {
                    state.candidate_sha = Some(sha.clone());
                }
                if let Some(evidence_id) = evidence_id {
                    state.evidence.insert(
                        evidence_id.clone(),
                        state.candidate_sha.clone().unwrap_or_default(),
                    );
                }
            }
            Ok(())
        }
        TeamEventKind::AgentAttached => {
            let agent_thread_id = event.agent_thread_id.clone().ok_or_else(|| {
                TeamRuntimeError::invalid("agent attached event missing agent_thread_id")
            })?;
            let node_run_id = event.node_run_id.clone().ok_or_else(|| {
                TeamRuntimeError::invalid("agent attached event missing node_run_id")
            })?;
            let node_id = event
                .node_id
                .clone()
                .unwrap_or_else(|| state.current_node_id.clone());
            state.agents.insert(
                agent_thread_id.clone(),
                TeamAgentBinding {
                    team_session_id: state.team_session_id.clone(),
                    node_run_id,
                    node_id,
                    role: event.role.clone().unwrap_or_default(),
                    agent_thread_id,
                },
            );
            state.lifecycle = TeamLifecycle::WaitingAgent;
            Ok(())
        }
        TeamEventKind::AgentCompleted | TeamEventKind::AgentInterrupted => {
            if let Some(agent_id) = &event.agent_thread_id {
                state.agents.remove(agent_id);
            }
            if state.agents.is_empty() && state.lifecycle == TeamLifecycle::WaitingAgent {
                state.lifecycle = TeamLifecycle::Running;
            }
            Ok(())
        }
        TeamEventKind::EvidenceRecorded => {
            if let TeamEventPayload::Evidence {
                evidence_id,
                identity,
            } = &event.payload
            {
                state
                    .evidence
                    .insert(evidence_id.clone(), identity.clone().unwrap_or_default());
            }
            Ok(())
        }
        TeamEventKind::EvidenceInvalidated => {
            if let TeamEventPayload::Evidence { evidence_id, .. } = &event.payload {
                state.evidence.remove(evidence_id);
            }
            Ok(())
        }
        TeamEventKind::EvidenceReused => Ok(()),
        TeamEventKind::TransitionSelected => {
            if let TeamEventPayload::Transition { to, result, .. } = &event.payload {
                if let Some(to) = to {
                    let node_id = to.parse().map_err(TeamRuntimeError::invalid)?;
                    state.current_node_id = node_id;
                    state.current_node_run = None;
                }
                if let Some(result) = result {
                    state.last_result = Some(result.clone());
                }
            }
            state.lifecycle = TeamLifecycle::Running;
            Ok(())
        }
        TeamEventKind::DeviationRecorded => {
            state.lifecycle = TeamLifecycle::NeedsAttention;
            Ok(())
        }
        TeamEventKind::ExternalWaitEntered => {
            if let TeamEventPayload::ExternalWait { reason } = &event.payload {
                state.waiting_reason = Some(reason.clone());
            }
            state.lifecycle = TeamLifecycle::WaitingExternal;
            Ok(())
        }
        TeamEventKind::ExternalWaitResolved => {
            state.waiting_reason = None;
            state.lifecycle = TeamLifecycle::Running;
            Ok(())
        }
        TeamEventKind::TransitionRecommended
        | TeamEventKind::ToolOperationStarted
        | TeamEventKind::ToolOperationCompleted
        | TeamEventKind::ToolOperationFailed
        | TeamEventKind::ToolCoverageUnreported => Ok(()),
    }
}

#[allow(dead_code)]
pub fn replay(
    mut state: TeamSessionState,
    events: &[TeamEvent],
) -> TeamRuntimeResult<TeamSessionState> {
    for event in events {
        if event.sequence < state.next_sequence {
            continue;
        }
        reduce(&mut state, event)?;
    }
    Ok(state)
}
