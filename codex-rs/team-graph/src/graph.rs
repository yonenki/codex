use crate::dto::TeamGraphToml;
use crate::ids::MetricEffect;
use crate::ids::NodeId;
use crate::ids::RoleName;
use crate::ids::ToolCapability;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct GraphHash(String);

impl GraphHash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for GraphHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSummary {
    pub name: String,
    pub version: String,
    pub description: String,
    pub schema_version: u32,
    pub hash: GraphHash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamGraph {
    pub schema_version: u32,
    pub name: String,
    pub version: String,
    pub description: String,
    pub start: NodeId,
    pub terminals: Vec<NodeId>,
    pub nodes: BTreeMap<NodeId, TeamNode>,
}

impl TeamGraph {
    pub fn summary(&self) -> GraphSummary {
        GraphSummary {
            name: self.name.clone(),
            version: self.version.clone(),
            description: self.description.clone(),
            schema_version: self.schema_version,
            hash: hash_graph(self),
        }
    }

    pub fn node(&self, id: &NodeId) -> Option<&TeamNode> {
        self.nodes.get(id)
    }

    pub fn is_terminal(&self, id: &NodeId) -> bool {
        self.terminals.iter().any(|terminal| terminal == id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamNode {
    pub id: NodeId,
    pub purpose: String,
    pub role: Option<RoleName>,
    pub prompt: String,
    pub completion: String,
    pub available_tools: Vec<ToolCapability>,
    pub recommended_tools: Vec<ToolCapability>,
    pub transitions: Vec<TeamTransition>,
}

impl TeamNode {
    pub fn recommended_transitions(&self) -> impl Iterator<Item = &TeamTransition> {
        self.transitions
            .iter()
            .filter(|transition| transition.recommended)
    }

    pub fn transition_for(&self, result: &str) -> Option<&TeamTransition> {
        self.transitions
            .iter()
            .find(|transition| transition.on == result)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamTransition {
    pub on: String,
    pub to: NodeId,
    pub recommended: bool,
    pub guide: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metric_effects: Vec<MetricEffect>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGuide {
    pub node_id: NodeId,
    pub purpose: String,
    pub role: Option<RoleName>,
    pub prompt: String,
    pub completion: String,
    pub available_tools: Vec<ToolCapability>,
    pub recommended_tools: Vec<ToolCapability>,
    pub possible_transitions: Vec<TeamTransition>,
    pub recommended_transitions: Vec<TeamTransition>,
}

impl NodeGuide {
    pub fn from_node(node: &TeamNode) -> Self {
        Self {
            node_id: node.id.clone(),
            purpose: node.purpose.clone(),
            role: node.role.clone(),
            prompt: node.prompt.clone(),
            completion: node.completion.clone(),
            available_tools: node.available_tools.clone(),
            recommended_tools: node.recommended_tools.clone(),
            possible_transitions: node.transitions.clone(),
            recommended_transitions: node.recommended_transitions().cloned().collect(),
        }
    }
}

pub fn hash_graph(graph: &TeamGraph) -> GraphHash {
    let bytes = serde_json::to_vec(graph).unwrap_or_default();
    let digest = Sha256::digest(bytes);
    GraphHash(format!("{digest:x}"))
}

impl TryFrom<TeamGraphToml> for TeamGraph {
    type Error = String;

    fn try_from(value: TeamGraphToml) -> Result<Self, Self::Error> {
        let mut nodes = BTreeMap::new();
        for node in value.nodes {
            let id = NodeId::new(node.id)?;
            if nodes.contains_key(&id) {
                return Err(format!("duplicate node id '{}'", id.as_str()));
            }
            let available_tools = parse_tools(&node.available_tools)?;
            let recommended_tools = parse_tools(&node.recommended_tools)?;
            let transitions = node
                .transitions
                .into_iter()
                .map(|transition| {
                    Ok(TeamTransition {
                        on: transition.on,
                        to: NodeId::new(transition.to)?,
                        recommended: transition.recommended,
                        guide: transition.guide,
                        metric_effects: transition.metric_effects,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            nodes.insert(
                id.clone(),
                TeamNode {
                    id,
                    purpose: node.purpose,
                    role: node.role.map(RoleName::new).transpose()?,
                    prompt: node.prompt,
                    completion: node.completion,
                    available_tools,
                    recommended_tools,
                    transitions,
                },
            );
        }
        Ok(Self {
            schema_version: value.schema_version,
            name: value.name,
            version: value.version,
            description: value.description,
            start: NodeId::new(value.start)?,
            terminals: value
                .terminals
                .into_iter()
                .map(NodeId::new)
                .collect::<Result<Vec<_>, _>>()?,
            nodes,
        })
    }
}

fn parse_tools(values: &[String]) -> Result<Vec<ToolCapability>, String> {
    let mut tools = Vec::new();
    for value in values {
        let capability = ToolCapability::parse(value)?;
        if !tools.contains(&capability) {
            tools.push(capability);
        }
    }
    Ok(tools)
}
