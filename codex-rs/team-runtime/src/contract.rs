use crate::event::TeamEvent;
use crate::ids::TeamSessionId;
use serde::Deserialize;
use serde::Serialize;

pub const TEAM_EVENTS_CONTRACT_VERSION: &str = "team-events.v1";

/// agent-collab の team event ingest が 1 リクエストで受理する最大件数。
/// 両リポジトリで同じ wire 上限を保ち、超過 batch を 400 で落とさない。
pub const TEAM_EVENTS_MAX_BATCH: usize = 100;

/// Versioned ingest payload that agent-collab should accept idempotently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamEventsBatch {
    pub contract_version: String,
    pub agent_id: String,
    pub events: Vec<TeamEventEnvelope>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamEventEnvelope {
    pub event_id: String,
    pub team_session_id: TeamSessionId,
    pub sequence: u64,
    pub kind: String,
    pub occurred_at: String,
    pub graph_name: String,
    pub graph_version: String,
    pub graph_hash: String,
    pub node_id: Option<String>,
    pub node_run_id: Option<String>,
    pub attempt: Option<u32>,
    pub agent_thread_id: Option<String>,
    pub role: Option<String>,
    pub payload: serde_json::Value,
}

impl TeamEventEnvelope {
    pub fn from_event(event: &TeamEvent) -> Self {
        Self {
            event_id: event.event_id.to_string(),
            team_session_id: event.team_session_id.clone(),
            sequence: event.sequence,
            kind: serde_json::to_value(event.kind)
                .ok()
                .and_then(|value| value.as_str().map(str::to_owned))
                .unwrap_or_else(|| format!("{:?}", event.kind)),
            occurred_at: event.occurred_at.to_rfc3339(),
            graph_name: event.graph_name.clone(),
            graph_version: event.graph_version.clone(),
            graph_hash: event.graph_hash.to_string(),
            node_id: event.node_id.as_ref().map(ToString::to_string),
            node_run_id: event.node_run_id.as_ref().map(ToString::to_string),
            attempt: event.attempt,
            agent_thread_id: event.agent_thread_id.clone(),
            role: event.role.clone(),
            payload: serde_json::to_value(&event.payload).unwrap_or(serde_json::Value::Null),
        }
    }
}

pub fn team_events_path(server_url: &str, agent_id: &str) -> String {
    let base = server_url.trim_end_matches('/');
    format!(
        "{base}/api/agents/{}/team-events",
        urlencoding_lite(agent_id)
    )
}

fn urlencoding_lite(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
