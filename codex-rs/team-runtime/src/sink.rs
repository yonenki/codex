use crate::contract::TEAM_EVENTS_CONTRACT_VERSION;
use crate::contract::TeamEventEnvelope;
use crate::contract::TeamEventsBatch;
use crate::contract::team_events_path;
use crate::error::TeamRuntimeError;
use crate::error::TeamRuntimeResult;
use crate::event::TeamEvent;
use std::sync::Arc;
use std::sync::Mutex;

pub trait TeamEventSink: Send + Sync {
    fn publish(
        &self,
        events: &[TeamEvent],
    ) -> impl std::future::Future<Output = TeamRuntimeResult<()>> + Send;
}

#[derive(Default)]
pub struct RecordingSink {
    events: Mutex<Vec<TeamEventEnvelope>>,
}

impl RecordingSink {
    pub fn envelopes(&self) -> Vec<TeamEventEnvelope> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TeamEventSink for RecordingSink {
    async fn publish(&self, events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        let mut stored = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stored.extend(events.iter().map(TeamEventEnvelope::from_event));
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct FailingSink {
    remaining_failures: Arc<Mutex<u32>>,
}

impl FailingSink {
    pub fn fail_times(times: u32) -> Self {
        Self {
            remaining_failures: Arc::new(Mutex::new(times)),
        }
    }
}

impl TeamEventSink for FailingSink {
    async fn publish(&self, _events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        let mut remaining = self
            .remaining_failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *remaining > 0 {
            *remaining -= 1;
            return Err(TeamRuntimeError::Sink("sink unavailable".to_string()));
        }
        Ok(())
    }
}

pub struct HttpTeamEventSink {
    client: reqwest::Client,
    server_url: String,
    agent_id: String,
    report_token: String,
}

impl HttpTeamEventSink {
    pub fn from_env() -> Option<Self> {
        let server_url = std::env::var("AGENT_COLLAB_SERVER_URL").ok()?;
        let agent_id = std::env::var("AGENT_COLLAB_AGENT_ID").ok()?;
        let report_token = std::env::var("AGENT_COLLAB_REPORT_TOKEN").ok()?;
        if server_url.is_empty() || agent_id.is_empty() || report_token.is_empty() {
            return None;
        }
        Some(Self {
            client: reqwest::Client::new(),
            server_url,
            agent_id,
            report_token,
        })
    }
}

/// agent-collab の既存報告環境が無いときは成功扱いにせず、outbox へ残す。
pub struct EnvTeamEventSink {
    inner: Option<HttpTeamEventSink>,
}

impl EnvTeamEventSink {
    pub fn from_process_env() -> Self {
        Self {
            inner: HttpTeamEventSink::from_env(),
        }
    }

    pub fn is_configured(&self) -> bool {
        self.inner.is_some()
    }
}

impl TeamEventSink for EnvTeamEventSink {
    async fn publish(&self, events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        match &self.inner {
            Some(sink) => sink.publish(events).await,
            None => Err(TeamRuntimeError::Sink(
                "AGENT_COLLAB_SERVER_URL, AGENT_COLLAB_AGENT_ID, and AGENT_COLLAB_REPORT_TOKEN are required".to_string(),
            )),
        }
    }
}

impl TeamEventSink for HttpTeamEventSink {
    async fn publish(&self, events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        let batch = TeamEventsBatch {
            contract_version: TEAM_EVENTS_CONTRACT_VERSION.to_string(),
            agent_id: self.agent_id.clone(),
            events: events.iter().map(TeamEventEnvelope::from_event).collect(),
        };
        let url = team_events_path(&self.server_url, &self.agent_id);
        let response = self
            .client
            .post(url)
            .header("content-type", "application/json")
            .header("x-agent-collab-report-token", &self.report_token)
            .json(&batch)
            .send()
            .await
            .map_err(|err| TeamRuntimeError::Sink(err.to_string()))?;
        if !response.status().is_success() {
            return Err(TeamRuntimeError::Sink(format!(
                "team event ingest returned {}",
                response.status()
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::TeamEvent;
    use crate::event::TeamEventKind;
    use crate::event::TeamEventPayload;
    use crate::ids::TeamSessionId;
    use pretty_assertions::assert_eq;

    #[test]
    fn contract_version_is_stable() {
        assert_eq!(TEAM_EVENTS_CONTRACT_VERSION, "team-events.v1");
        assert_eq!(
            team_events_path("http://127.0.0.1:9/", "pane 1"),
            "http://127.0.0.1:9/api/agents/pane%201/team-events"
        );
    }

    #[tokio::test]
    async fn recording_sink_preserves_event_identity() {
        let sink = RecordingSink::default();
        let event = TeamEvent::new(
            TeamSessionId::generate(),
            1,
            TeamEventKind::TeamStarted,
            "sample".into(),
            "1".into(),
            crate::tests_support::sample_hash(),
            TeamEventPayload::TeamStarted {
                task_ref: None,
                worktree: None,
                branch: None,
            },
        );
        sink.publish(std::slice::from_ref(&event))
            .await
            .expect("publish");
        let envelopes = sink.envelopes();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].event_id, event.event_id.to_string());
        assert_eq!(envelopes[0].sequence, 1);
    }
}
