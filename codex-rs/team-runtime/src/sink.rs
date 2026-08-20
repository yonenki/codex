use crate::contract::TEAM_EVENTS_CONTRACT_VERSION;
use crate::contract::TeamEventEnvelope;
use crate::contract::TeamEventsBatch;
use crate::contract::team_events_path;
use crate::error::TeamRuntimeError;
use crate::error::TeamRuntimeResult;
use crate::event::TeamEvent;
use std::sync::Arc;
use std::sync::Mutex;

const TEAM_EVENTS_MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;

pub trait TeamEventSink: Send + Sync {
    fn publish(
        &self,
        events: &[TeamEvent],
    ) -> impl std::future::Future<Output = TeamRuntimeResult<()>> + Send;
}

#[derive(Clone, Default)]
pub struct RecordingSink {
    events: std::sync::Arc<Mutex<Vec<TeamEventEnvelope>>>,
    batches: std::sync::Arc<Mutex<Vec<Vec<TeamEventEnvelope>>>>,
}

impl RecordingSink {
    pub fn envelopes(&self) -> Vec<TeamEventEnvelope> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn batches(&self) -> Vec<Vec<TeamEventEnvelope>> {
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl TeamEventSink for RecordingSink {
    async fn publish(&self, events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        let batch: Vec<TeamEventEnvelope> =
            events.iter().map(TeamEventEnvelope::from_event).collect();
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(batch.iter().cloned());
        self.batches
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(batch);
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

/// 次の publish を保留する。attach 永続化後の同期 outbox 待ちを再現する。
#[derive(Default)]
pub struct HoldNextPublishSink {
    hold: Mutex<HoldNextPublish>,
}

#[derive(Default)]
struct HoldNextPublish {
    release: Option<tokio::sync::oneshot::Receiver<()>>,
    entered: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HoldNextPublishSink {
    pub fn hold_next(
        &self,
    ) -> (
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    ) {
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        *self
            .hold
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = HoldNextPublish {
            release: Some(release_rx),
            entered: Some(entered_tx),
        };
        (release_tx, entered_rx)
    }
}

impl TeamEventSink for HoldNextPublishSink {
    async fn publish(&self, _events: &[TeamEvent]) -> TeamRuntimeResult<()> {
        let (release, entered) = {
            let mut hold = self
                .hold
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (hold.release.take(), hold.entered.take())
        };
        if let Some(entered) = entered {
            let _ = entered.send(());
        }
        if let Some(release) = release {
            let _ = release.await;
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
        let url = team_events_path(&self.server_url, &self.agent_id);
        for body in serialized_http_batches(&self.agent_id, events)? {
            let response = self
                .client
                .post(&url)
                .header("content-type", "application/json")
                .header("x-agent-collab-report-token", &self.report_token)
                .body(body)
                .send()
                .await
                .map_err(|err| TeamRuntimeError::Sink(err.to_string()))?;
            if !response.status().is_success() {
                return Err(TeamRuntimeError::Sink(format!(
                    "team event ingest returned {}",
                    response.status()
                )));
            }
        }
        Ok(())
    }
}

fn serialized_http_batches(
    agent_id: &str,
    events: &[TeamEvent],
) -> TeamRuntimeResult<Vec<Vec<u8>>> {
    let mut bodies = Vec::new();
    serialize_http_chunk(agent_id, events, &mut bodies)?;
    Ok(bodies)
}

fn serialize_http_chunk(
    agent_id: &str,
    events: &[TeamEvent],
    bodies: &mut Vec<Vec<u8>>,
) -> TeamRuntimeResult<()> {
    let batch = TeamEventsBatch {
        contract_version: TEAM_EVENTS_CONTRACT_VERSION.to_string(),
        agent_id: agent_id.to_string(),
        events: events.iter().map(TeamEventEnvelope::from_event).collect(),
    };
    let body = serde_json::to_vec(&batch).map_err(|err| TeamRuntimeError::Sink(err.to_string()))?;
    if body.len() <= TEAM_EVENTS_MAX_HTTP_BODY_BYTES {
        bodies.push(body);
        return Ok(());
    }
    if events.len() == 1 {
        return Err(TeamRuntimeError::Sink(format!(
            "serialized team event exceeds the {TEAM_EVENTS_MAX_HTTP_BODY_BYTES}-byte HTTP body limit"
        )));
    }
    let middle = events.len() / 2;
    serialize_http_chunk(agent_id, &events[..middle], bodies)?;
    serialize_http_chunk(agent_id, &events[middle..], bodies)
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
        assert_eq!(crate::TEAM_EVENTS_MAX_BATCH, 100);
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

    #[test]
    fn http_batches_preserve_long_messages_within_consumer_body_limit() {
        let exact_message = "x".repeat(200_000);
        let events: Vec<_> = (0..100)
            .map(|sequence| {
                TeamEvent::new(
                    TeamSessionId::generate(),
                    sequence,
                    TeamEventKind::AgentAttached,
                    "sample".into(),
                    "1".into(),
                    crate::tests_support::sample_hash(),
                    TeamEventPayload::AgentAttached {
                        role: "worker".into(),
                        backend_fallback: None,
                        backend: Some(crate::AgentBackend::Native),
                        harness: None,
                        model: Some("resolved-model".into()),
                        delegation_message: Some(exact_message.clone()),
                    },
                )
            })
            .collect();

        let bodies = serialized_http_batches("agent-1", &events).expect("serialize batches");
        assert_eq!(bodies.len(), 2);
        assert!(
            bodies
                .iter()
                .all(|body| body.len() <= TEAM_EVENTS_MAX_HTTP_BODY_BYTES)
        );
        let decoded: Vec<TeamEventsBatch> = bodies
            .iter()
            .map(|body| serde_json::from_slice(body).expect("decode batch"))
            .collect();
        assert_eq!(
            decoded
                .iter()
                .map(|batch| batch.events.len())
                .sum::<usize>(),
            100
        );
        assert_eq!(
            decoded[0].events[0].payload["delegation_message"],
            exact_message
        );
    }
}
