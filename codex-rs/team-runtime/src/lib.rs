//! Team session runtime: events, reducer, store, outbox, and TeamControl.

mod binding;
mod contract;
mod control;
mod error;
mod event;
mod ids;
mod reducer;
mod sink;
mod state;
mod store;

pub use binding::AgentBackend;
pub use binding::AgentBackendIdentity;
pub use binding::PendingAgentAttachMetadata;
pub use binding::PendingTeamBinding;
pub use binding::TeamAgentBinding;
pub use contract::TEAM_EVENTS_CONTRACT_VERSION;
pub use contract::TEAM_EVENTS_MAX_BATCH;
pub use contract::TeamEventEnvelope;
pub use contract::TeamEventsBatch;
pub use contract::team_events_path;
pub use control::EndTeamCommand;
pub use control::EvidenceCommand;
pub use control::ExternalWaitCommand;
pub use control::RecordResultCommand;
pub use control::StartNodeCommand;
pub use control::StartTeamCommand;
pub use control::TeamControl;
pub use control::TeamView;
pub use control::TransitionCommand;
pub use error::TeamRuntimeError;
pub use error::TeamRuntimeResult;
pub use event::TeamEvent;
pub use event::TeamEventKind;
pub use event::TeamEventPayload;
pub use ids::EventId;
pub use ids::NodeRunId;
pub use ids::StateRevision;
pub use ids::TeamSessionId;
pub use reducer::reduce;
pub use sink::EnvTeamEventSink;
pub use sink::FailingSink;
pub use sink::HttpTeamEventSink;
pub use sink::RecordingSink;
pub use sink::TeamEventSink;
pub use state::NodeRun;
pub use state::TeamLifecycle;
pub use state::TeamSessionState;
pub use store::LazySqliteTeamStore;
pub use store::MemoryTeamStore;
pub use store::SqliteTeamStore;
pub use store::TeamStore;

#[cfg(test)]
mod control_tests;
#[cfg(test)]
mod reducer_tests;
#[cfg(test)]
mod store_tests;
#[cfg(test)]
pub(crate) mod tests_support;
