use super::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvanceTeamCommand {
    #[serde(flatten)]
    pub completion: RecordResultCommand,
    pub deviation_reason: Option<String>,
}

impl TeamControl {
    /// Complete a node and select its declared successor in one durable transaction.
    /// No result is inferred: the caller supplies the verdict and its evidence.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "Serialize revision checks, the durable commit, and publication of the committed snapshot."
    )]
    pub async fn advance_team(&self, command: AdvanceTeamCommand) -> TeamRuntimeResult<TeamView> {
        self.ensure_restored().await?;
        let completion = command.completion;
        let mut teams = self.teams.lock().await;
        let state = teams
            .get_mut(&completion.team_session_id)
            .ok_or_else(|| TeamRuntimeError::TeamNotFound(completion.team_session_id.clone()))?;
        require_open(state)?;
        if state.revision != completion.expected_revision {
            return Err(TeamRuntimeError::StaleRevision {
                expected: completion.expected_revision,
                actual: state.revision,
            });
        }
        if !state.agents.is_empty() {
            return Err(TeamRuntimeError::ActiveAgents(
                state.team_session_id.clone(),
            ));
        }
        if state.waiting_reason.is_some() {
            return Err(TeamRuntimeError::invalid(
                "resolve the external wait before advancing",
            ));
        }
        let node = state
            .graph
            .node(&state.current_node_id)
            .ok_or_else(|| TeamRuntimeError::invalid("current node missing from graph snapshot"))?;
        let transition = node
            .transition_for(&completion.result)
            .ok_or_else(|| {
                TeamRuntimeError::invalid(format!(
                    "result '{}' is not a declared transition from '{}'",
                    completion.result, state.current_node_id
                ))
            })?
            .clone();
        if !transition.recommended
            && command
                .deviation_reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err(TeamRuntimeError::invalid(
                "non-recommended transition requires deviation_reason",
            ));
        }

        let mut next = state.clone();
        let mut events = Vec::new();
        if next.current_node_run.is_none() {
            stage_start(&mut next, &mut events)?;
        }
        require_active_node_run(&next)?;
        let event = event_from_state(
            &next,
            TeamEventKind::NodeCompleted,
            TeamEventPayload::NodeCompleted {
                result: completion.result.clone(),
                candidate_sha: completion.candidate_sha,
                evidence_id: completion.evidence_id,
                qa: completion.qa,
                findings: completion.findings,
            },
        );
        stage(&mut next, &mut events, event)?;
        let event = event_from_state(
            &next,
            TeamEventKind::TransitionSelected,
            TeamEventPayload::Transition {
                result: Some(completion.result),
                to: Some(transition.to.to_string()),
                recommended: transition.recommended,
                deviation_reason: command.deviation_reason,
                metric_effects: transition.metric_effects,
            },
        );
        stage(&mut next, &mut events, event)?;
        if !next.graph.is_terminal(&next.current_node_id) {
            stage_start(&mut next, &mut events)?;
        }
        self.store.persist_events(next.clone(), events).await?;
        *state = next;
        let view = view_from_state(state);
        drop(teams);
        self.refresh_surface().await;
        self.flush_committed_outbox().await?;
        Ok(view)
    }
}

fn stage_start(state: &mut TeamSessionState, events: &mut Vec<TeamEvent>) -> TeamRuntimeResult<()> {
    let node = state
        .graph
        .node(&state.current_node_id)
        .ok_or_else(|| TeamRuntimeError::invalid("current node missing from graph snapshot"))?;
    let mut event = event_from_state(
        state,
        TeamEventKind::NodeStarted,
        TeamEventPayload::NodeStarted {
            purpose: node.purpose.clone(),
        },
    );
    event.node_run_id = Some(NodeRunId::generate());
    event.attempt = Some(1);
    event.role = node.role.as_ref().map(ToString::to_string);
    stage(state, events, event)
}

fn stage(
    state: &mut TeamSessionState,
    events: &mut Vec<TeamEvent>,
    event: TeamEvent,
) -> TeamRuntimeResult<()> {
    reduce(state, &event)?;
    events.push(event);
    Ok(())
}
