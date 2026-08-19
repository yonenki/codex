use crate::binding::TeamAgentBinding;
use crate::error::TeamRuntimeError;
use crate::error::TeamRuntimeResult;
use crate::event::TeamEvent;
use crate::ids::EventId;
use crate::ids::TeamSessionId;
use crate::state::TeamSessionState;
use sqlx::Executor;
use sqlx::Row;
use sqlx::SqlitePool;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::sqlite::SqlitePoolOptions;
use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;
use tokio::sync::OnceCell;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS team_events (
    event_id TEXT PRIMARY KEY,
    team_session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE (team_session_id, sequence)
);
CREATE TABLE IF NOT EXISTS team_outbox (
    event_id TEXT PRIMARY KEY,
    team_session_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload TEXT NOT NULL,
    sent INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS team_snapshots (
    team_session_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS team_bindings (
    agent_thread_id TEXT PRIMARY KEY,
    payload TEXT NOT NULL
);
"#;

pub trait TeamStore: Send + Sync {
    fn persist_event(
        &self,
        state: &TeamSessionState,
        event: &TeamEvent,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<()>> + Send;

    fn load_teams(
        &self,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<Vec<TeamSessionState>>> + Send;

    fn load_events(
        &self,
        team_session_id: &TeamSessionId,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<Vec<TeamEvent>>> + Send;

    fn pending_outbox(
        &self,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<Vec<TeamEvent>>> + Send;

    fn mark_outbox_sent(
        &self,
        event_ids: &[EventId],
    ) -> impl std::future::Future<Output = TeamRuntimeResult<()>> + Send;

    fn persist_binding(
        &self,
        binding: &TeamAgentBinding,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<()>> + Send;

    fn load_bindings(
        &self,
    ) -> impl std::future::Future<Output = TeamRuntimeResult<Vec<TeamAgentBinding>>> + Send;
}

#[derive(Default)]
pub struct MemoryTeamStore {
    inner: Mutex<MemoryInner>,
}

#[derive(Default)]
struct MemoryInner {
    events: Vec<TeamEvent>,
    outbox: Vec<TeamEvent>,
    snapshots: BTreeMap<String, TeamSessionState>,
    bindings: BTreeMap<String, TeamAgentBinding>,
}

impl TeamStore for MemoryTeamStore {
    async fn persist_event(
        &self,
        state: &TeamSessionState,
        event: &TeamEvent,
    ) -> TeamRuntimeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.events.push(event.clone());
        inner.outbox.push(event.clone());
        inner
            .snapshots
            .insert(state.team_session_id.to_string(), state.clone());
        Ok(())
    }

    async fn load_teams(&self) -> TeamRuntimeResult<Vec<TeamSessionState>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner.snapshots.values().cloned().collect())
    }

    async fn load_events(
        &self,
        team_session_id: &TeamSessionId,
    ) -> TeamRuntimeResult<Vec<TeamEvent>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner
            .events
            .iter()
            .filter(|event| event.team_session_id == *team_session_id)
            .cloned()
            .collect())
    }

    async fn pending_outbox(&self) -> TeamRuntimeResult<Vec<TeamEvent>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner.outbox.clone())
    }

    async fn mark_outbox_sent(&self, event_ids: &[EventId]) -> TeamRuntimeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .outbox
            .retain(|event| !event_ids.contains(&event.event_id));
        Ok(())
    }

    async fn persist_binding(&self, binding: &TeamAgentBinding) -> TeamRuntimeResult<()> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .bindings
            .insert(binding.agent_thread_id.clone(), binding.clone());
        Ok(())
    }

    async fn load_bindings(&self) -> TeamRuntimeResult<Vec<TeamAgentBinding>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner.bindings.values().cloned().collect())
    }
}

#[derive(Clone)]
pub struct SqliteTeamStore {
    pool: SqlitePool,
}

impl SqliteTeamStore {
    pub async fn open(path: &Path) -> TeamRuntimeResult<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        Self::connect(options).await
    }

    pub async fn memory() -> TeamRuntimeResult<Self> {
        let options = SqliteConnectOptions::new()
            .filename(":memory:")
            .create_if_missing(true);
        Self::connect(options).await
    }

    async fn connect(options: SqliteConnectOptions) -> TeamRuntimeResult<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        pool.execute(SCHEMA)
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        Ok(Self { pool })
    }
}

/// 起動時は同期的に開き、最初の persist/restore で SQLite へ接続する。
pub struct LazySqliteTeamStore {
    path: PathBuf,
    inner: OnceCell<SqliteTeamStore>,
}

impl LazySqliteTeamStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            inner: OnceCell::const_new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn store(&self) -> TeamRuntimeResult<&SqliteTeamStore> {
        self.inner
            .get_or_try_init(|| SqliteTeamStore::open(&self.path))
            .await
    }
}

impl TeamStore for LazySqliteTeamStore {
    async fn persist_event(
        &self,
        state: &TeamSessionState,
        event: &TeamEvent,
    ) -> TeamRuntimeResult<()> {
        self.store().await?.persist_event(state, event).await
    }

    async fn load_teams(&self) -> TeamRuntimeResult<Vec<TeamSessionState>> {
        self.store().await?.load_teams().await
    }

    async fn load_events(
        &self,
        team_session_id: &TeamSessionId,
    ) -> TeamRuntimeResult<Vec<TeamEvent>> {
        self.store().await?.load_events(team_session_id).await
    }

    async fn pending_outbox(&self) -> TeamRuntimeResult<Vec<TeamEvent>> {
        self.store().await?.pending_outbox().await
    }

    async fn mark_outbox_sent(&self, event_ids: &[EventId]) -> TeamRuntimeResult<()> {
        self.store().await?.mark_outbox_sent(event_ids).await
    }

    async fn persist_binding(&self, binding: &TeamAgentBinding) -> TeamRuntimeResult<()> {
        self.store().await?.persist_binding(binding).await
    }

    async fn load_bindings(&self) -> TeamRuntimeResult<Vec<TeamAgentBinding>> {
        self.store().await?.load_bindings().await
    }
}

impl TeamStore for SqliteTeamStore {
    async fn persist_event(
        &self,
        state: &TeamSessionState,
        event: &TeamEvent,
    ) -> TeamRuntimeResult<()> {
        let event_json =
            serde_json::to_string(event).map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        let state_json =
            serde_json::to_string(state).map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        sqlx::query(
            "INSERT INTO team_events (event_id, team_session_id, sequence, payload) VALUES (?, ?, ?, ?)",
        )
        .bind(event.event_id.as_str())
        .bind(event.team_session_id.as_str())
        .bind(event.sequence as i64)
        .bind(&event_json)
        .execute(&mut *tx)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        sqlx::query(
            "INSERT INTO team_outbox (event_id, team_session_id, sequence, payload, sent) VALUES (?, ?, ?, ?, 0)",
        )
        .bind(event.event_id.as_str())
        .bind(event.team_session_id.as_str())
        .bind(event.sequence as i64)
        .bind(&event_json)
        .execute(&mut *tx)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        sqlx::query(
            "INSERT INTO team_snapshots (team_session_id, payload) VALUES (?, ?)
             ON CONFLICT(team_session_id) DO UPDATE SET payload = excluded.payload",
        )
        .bind(state.team_session_id.as_str())
        .bind(&state_json)
        .execute(&mut *tx)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        tx.commit()
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        Ok(())
    }

    async fn load_teams(&self) -> TeamRuntimeResult<Vec<TeamSessionState>> {
        let rows = sqlx::query("SELECT payload FROM team_snapshots")
            .fetch_all(&self.pool)
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload)
                    .map_err(|err| TeamRuntimeError::Store(err.to_string()))
            })
            .collect()
    }

    async fn load_events(
        &self,
        team_session_id: &TeamSessionId,
    ) -> TeamRuntimeResult<Vec<TeamEvent>> {
        let rows = sqlx::query(
            "SELECT payload FROM team_events WHERE team_session_id = ? ORDER BY sequence ASC",
        )
        .bind(team_session_id.as_str())
        .fetch_all(&self.pool)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload)
                    .map_err(|err| TeamRuntimeError::Store(err.to_string()))
            })
            .collect()
    }

    async fn pending_outbox(&self) -> TeamRuntimeResult<Vec<TeamEvent>> {
        let rows = sqlx::query(
            "SELECT payload FROM team_outbox WHERE sent = 0 ORDER BY team_session_id, sequence ASC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload)
                    .map_err(|err| TeamRuntimeError::Store(err.to_string()))
            })
            .collect()
    }

    async fn mark_outbox_sent(&self, event_ids: &[EventId]) -> TeamRuntimeResult<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        for event_id in event_ids {
            sqlx::query("UPDATE team_outbox SET sent = 1 WHERE event_id = ?")
                .bind(event_id.as_str())
                .execute(&mut *tx)
                .await
                .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        }
        tx.commit()
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        Ok(())
    }

    async fn persist_binding(&self, binding: &TeamAgentBinding) -> TeamRuntimeResult<()> {
        let payload = serde_json::to_string(binding)
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        sqlx::query(
            "INSERT INTO team_bindings (agent_thread_id, payload) VALUES (?, ?)
             ON CONFLICT(agent_thread_id) DO UPDATE SET payload = excluded.payload",
        )
        .bind(&binding.agent_thread_id)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        Ok(())
    }

    async fn load_bindings(&self) -> TeamRuntimeResult<Vec<TeamAgentBinding>> {
        let rows = sqlx::query("SELECT payload FROM team_bindings")
            .fetch_all(&self.pool)
            .await
            .map_err(|err| TeamRuntimeError::Store(err.to_string()))?;
        rows.into_iter()
            .map(|row| {
                let payload: String = row.get("payload");
                serde_json::from_str(&payload)
                    .map_err(|err| TeamRuntimeError::Store(err.to_string()))
            })
            .collect()
    }
}
