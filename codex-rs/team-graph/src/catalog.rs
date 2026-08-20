use crate::dto::TeamGraphToml;
use crate::error::TeamGraphError;
use crate::error::TeamGraphResult;
use crate::graph::GraphSummary;
use crate::graph::TeamGraph;
use crate::validate_team_graph;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

#[derive(Clone, Debug, Default)]
pub struct TeamGraphCatalog {
    graphs: BTreeMap<String, TeamGraph>,
}

impl TeamGraphCatalog {
    pub fn new(graphs: impl IntoIterator<Item = TeamGraph>) -> Self {
        let mut mapped = BTreeMap::new();
        for graph in graphs {
            mapped.insert(graph.name.clone(), graph);
        }
        Self { graphs: mapped }
    }

    pub fn load_from_roots(
        roots: &[PathBuf],
        known_roles: &BTreeSet<String>,
    ) -> TeamGraphResult<Self> {
        let mut graphs = BTreeMap::new();
        for root in roots {
            for graph in load_graphs_from_dir(root, known_roles)? {
                if graphs.contains_key(&graph.name) {
                    return Err(TeamGraphError::invalid(format!(
                        "duplicate team graph name '{}'",
                        graph.name
                    )));
                }
                graphs.insert(graph.name.clone(), graph);
            }
        }
        Ok(Self { graphs })
    }

    pub fn summaries(&self) -> Vec<GraphSummary> {
        self.graphs.values().map(TeamGraph::summary).collect()
    }

    pub fn get(&self, name: &str) -> Option<&TeamGraph> {
        self.graphs.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = &TeamGraph> {
        self.graphs.values()
    }
}

pub fn discover_team_graphs(
    cwd: &Path,
    codex_home: Option<&Path>,
    known_roles: &BTreeSet<String>,
) -> TeamGraphResult<TeamGraphCatalog> {
    let mut roots = vec![cwd.join(".codex").join("teams")];
    if let Some(home) = codex_home {
        roots.push(home.join("teams"));
    }
    TeamGraphCatalog::load_from_roots(&roots, known_roles)
}

pub fn load_known_roles(cwd: &Path, extra: impl IntoIterator<Item = String>) -> BTreeSet<String> {
    let mut roles = extra.into_iter().collect::<BTreeSet<_>>();
    let agents_dir = cwd.join(".codex").join("agents");
    let Ok(entries) = fs::read_dir(agents_dir) else {
        return roles;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
            continue;
        }
        let Ok(contents) = fs::read_to_string(&path) else {
            continue;
        };
        if let Some(name) = parse_role_name(&contents) {
            roles.insert(name);
        }
    }
    roles
}

fn load_graphs_from_dir(
    dir: &Path,
    known_roles: &BTreeSet<String>,
) -> TeamGraphResult<Vec<TeamGraph>> {
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut graphs = Vec::new();
    let mut entries = fs::read_dir(dir)
        .map_err(|source| TeamGraphError::Io {
            path: dir.to_path_buf(),
            source,
        })?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("toml"))
        .collect::<Vec<_>>();
    entries.sort();
    for path in entries {
        graphs.push(load_graph_file(&path, known_roles)?);
    }
    Ok(graphs)
}

fn load_graph_file(path: &Path, known_roles: &BTreeSet<String>) -> TeamGraphResult<TeamGraph> {
    let contents = fs::read_to_string(path).map_err(|source| TeamGraphError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let dto: TeamGraphToml = toml::from_str(&contents).map_err(|source| TeamGraphError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    let graph = TeamGraph::try_from(dto).map_err(TeamGraphError::invalid)?;
    validate_team_graph(&graph, known_roles)?;
    Ok(graph)
}

fn parse_role_name(contents: &str) -> Option<String> {
    contents.lines().find_map(|line| {
        let line = line.trim();
        let rest = line.strip_prefix("name")?;
        let rest = rest.trim_start();
        let rest = rest.strip_prefix('=')?;
        let value = rest.trim().trim_matches('"').trim_matches('\'');
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    })
}
