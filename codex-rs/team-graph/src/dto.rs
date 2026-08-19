use crate::ids::MetricEffect;
use serde::Deserialize;
use serde::Serialize;

/// File-format DTO for `.codex/teams/*.toml`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TeamGraphToml {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub description: String,
    pub start: String,
    pub terminals: Vec<String>,
    #[serde(default)]
    pub nodes: Vec<TeamNodeToml>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TeamNodeToml {
    pub id: String,
    pub purpose: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub prompt: String,
    #[serde(default)]
    pub completion: String,
    #[serde(default)]
    pub available_tools: Vec<String>,
    #[serde(default)]
    pub recommended_tools: Vec<String>,
    #[serde(default)]
    pub transitions: Vec<TeamTransitionToml>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TeamTransitionToml {
    pub on: String,
    pub to: String,
    #[serde(default)]
    pub recommended: bool,
    #[serde(default)]
    pub guide: String,
    #[serde(default)]
    pub metric_effects: Vec<MetricEffect>,
}
