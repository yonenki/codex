//! Typed Team Graph definitions, discovery, and validation.
//!
//! Serialization DTOs stay separate from the runtime graph model so additional
//! file formats can be added later without changing TeamControl.

mod catalog;
mod dto;
mod error;
mod graph;
mod ids;
mod validation;

pub use catalog::TeamGraphCatalog;
pub use catalog::discover_team_graphs;
pub use catalog::load_known_roles;
pub use dto::TeamGraphToml;
pub use error::TeamGraphError;
pub use error::TeamGraphResult;
pub use graph::GraphHash;
pub use graph::GraphSummary;
pub use graph::NodeGuide;
pub use graph::TeamGraph;
pub use graph::TeamNode;
pub use graph::TeamTransition;
pub use graph::hash_graph;
pub use ids::MetricEffect;
pub use ids::NodeId;
pub use ids::RoleName;
pub use ids::SUPPORTED_SCHEMA_VERSION;
pub use ids::ToolCapability;
pub use validation::validate_team_graph;

#[cfg(test)]
mod catalog_tests;
#[cfg(test)]
mod validation_tests;
