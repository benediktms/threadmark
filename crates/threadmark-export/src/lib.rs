//! Deterministic portable exports and handoff rendering.

use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use threadmark_domain::{
    AuditEvent, EdgeType, Effort, FindingStatus, GraphSnapshot, Lifecycle, NodeKind, Source,
    Validity,
};

#[derive(Debug, Error)]
pub enum ExportError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid export package: {0}")]
    InvalidPackage(String),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortableEffort {
    pub format_version: u32,
    pub effort: Effort,
    pub graph: GraphSnapshot,
    pub sources: Vec<Source>,
    #[serde(default)]
    pub events: Vec<AuditEvent>,
}

pub fn write_package(
    package: &PortableEffort,
    directory: &Path,
    include_events: bool,
) -> Result<(), ExportError> {
    fs::create_dir_all(directory.join("nodes"))?;
    fs::write(
        directory.join("effort.yaml"),
        serde_yaml::to_string(&package.effort)?,
    )?;

    let mut nodes = package.graph.nodes.clone();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    for node in nodes {
        let frontmatter = serde_yaml::to_string(&node)?;
        let body = format!("---\n{frontmatter}---\n\n{}\n", node.body);
        fs::write(
            directory.join("nodes").join(format!("{}.md", node.id)),
            body,
        )?;
    }

    let mut edges = package.graph.edges.clone();
    edges.sort_by(|left, right| left.id.cmp(&right.id));
    fs::write(directory.join("edges.yaml"), serde_yaml::to_string(&edges)?)?;

    let mut sources = package.sources.clone();
    sources.sort_by(|left, right| left.id.cmp(&right.id));
    fs::write(
        directory.join("sources.yaml"),
        serde_yaml::to_string(&sources)?,
    )?;
    fs::write(
        directory.join("findings.yaml"),
        serde_yaml::to_string(&package.graph.findings)?,
    )?;
    fs::write(
        directory.join("fog.yaml"),
        serde_yaml::to_string(&package.graph.fog_patches)?,
    )?;
    fs::write(
        directory.join("exit-criteria.yaml"),
        serde_yaml::to_string(&package.graph.exit_criteria)?,
    )?;
    fs::write(directory.join("handoff.md"), render_handoff(package))?;

    if include_events {
        let mut output = String::new();
        for event in &package.events {
            output.push_str(&serde_json::to_string(event)?);
            output.push('\n');
        }
        fs::write(directory.join("events.jsonl"), output)?;
    }
    Ok(())
}

pub fn read_package(directory: &Path) -> Result<PortableEffort, ExportError> {
    let effort: Effort = serde_yaml::from_str(&fs::read_to_string(directory.join("effort.yaml"))?)?;
    let edges = read_yaml_or_default(&directory.join("edges.yaml"))?;
    let sources = read_yaml_or_default(&directory.join("sources.yaml"))?;
    let findings = read_yaml_or_default(&directory.join("findings.yaml"))?;
    let fog_patches = read_yaml_or_default(&directory.join("fog.yaml"))?;
    let exit_criteria = read_yaml_or_default(&directory.join("exit-criteria.yaml"))?;
    let mut nodes = Vec::new();
    let node_directory = directory.join("nodes");
    if node_directory.exists() {
        let mut paths: Vec<_> = fs::read_dir(node_directory)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .collect();
        paths.sort();
        for path in paths {
            let content = fs::read_to_string(path)?;
            let yaml = content
                .strip_prefix("---\n")
                .and_then(|body| body.split_once("---\n").map(|parts| parts.0))
                .ok_or_else(|| {
                    ExportError::InvalidPackage("node file is missing YAML front matter".into())
                })?;
            nodes.push(serde_yaml::from_str(yaml)?);
        }
    }
    let events = if directory.join("events.jsonl").exists() {
        fs::read_to_string(directory.join("events.jsonl"))?
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![]
    };
    Ok(PortableEffort {
        format_version: 1,
        effort,
        graph: GraphSnapshot {
            nodes,
            edges,
            findings,
            fog_patches,
            exit_criteria,
            ..GraphSnapshot::default()
        },
        sources,
        events,
    })
}

pub fn render_handoff(package: &PortableEffort) -> String {
    ["overview", "nodes", "findings", "fog_patches", "edges"]
        .into_iter()
        .filter_map(|section| render_handoff_section(package, section, true))
        .collect()
}

#[must_use]
pub fn render_handoff_section(
    package: &PortableEffort,
    section: &str,
    complete: bool,
) -> Option<String> {
    let effort = &package.effort;
    let graph = &package.graph;
    let mut output = String::new();
    match section {
        "overview" => output.push_str(&format!(
            "# {} — reasoning handoff\n\n## Destination\n\n{}\n\n## Scope\n\n{}\n\n",
            effort.title, effort.destination, effort.scope_notes
        )),
        "nodes" => {
            section_nodes(
                &mut output,
                "Constraints",
                graph,
                NodeKind::Constraint,
                complete,
            );
            section_nodes(
                &mut output,
                "Decisions",
                graph,
                NodeKind::Decision,
                complete,
            );
            section_nodes(
                &mut output,
                "Active assumptions",
                graph,
                NodeKind::Assumption,
                complete,
            );
            section_nodes(&mut output, "Evidence", graph, NodeKind::Evidence, complete);
            section_nodes(
                &mut output,
                "Experiments",
                graph,
                NodeKind::Experiment,
                complete,
            );

            let open: Vec<_> = graph
                .nodes
                .iter()
                .filter(|node| matches!(node.lifecycle, Lifecycle::Open | Lifecycle::InProgress))
                .collect();
            if open.is_empty() && complete {
                output.push_str("## Residual uncertainty\n\n");
                output.push_str("None recorded.\n\n");
            } else if !open.is_empty() {
                output.push_str("## Residual uncertainty\n\n");
                for node in open {
                    output.push_str(&format!(
                        "- **{}** (`{}`): {}\n",
                        node.title, node.id, node.summary
                    ));
                }
                output.push('\n');
            }
        }
        "findings" => {
            let active: Vec<_> = graph
                .findings
                .iter()
                .filter(|finding| {
                    matches!(
                        finding.status,
                        FindingStatus::Proposed | FindingStatus::Accepted
                    )
                })
                .collect();
            if active.is_empty() && complete {
                output.push_str("## Active findings\n\n");
                output.push_str("None recorded.\n\n");
            } else if !active.is_empty() {
                output.push_str("## Active findings\n\n");
                for finding in active {
                    output.push_str(&format!("- **{}**: {}\n", finding.title, finding.detail));
                }
                output.push('\n');
            }
        }
        "fog_patches" => {
            if graph.fog_patches.is_empty() && complete {
                output.push_str("## Fog and out of scope\n\n");
                output.push_str("None recorded.\n");
            } else if !graph.fog_patches.is_empty() {
                output.push_str("## Fog and out of scope\n\n");
                for fog in &graph.fog_patches {
                    output.push_str(&format!(
                        "- **{}** ({}): {}\n",
                        fog.title,
                        fog.status.as_str(),
                        fog.description
                    ));
                }
            }
            if complete || !graph.fog_patches.is_empty() {
                output.push('\n');
            }
        }
        "edges" => {
            let edges: Vec<_> = graph
                .edges
                .iter()
                .filter(|edge| {
                    matches!(
                        edge.edge_type,
                        EdgeType::Assumes
                            | EdgeType::Supports
                            | EdgeType::Supersedes
                            | EdgeType::Contradicts
                    )
                })
                .collect();
            if complete || !edges.is_empty() {
                output.push_str("## Decision relationships\n\n");
                for edge in edges {
                    output.push_str(&format!(
                        "- `{}` {} `{}`\n",
                        edge.source_node_id, edge.edge_type, edge.target_node_id
                    ));
                }
            }
        }
        _ => return None,
    }
    Some(output)
}

fn section_nodes(
    output: &mut String,
    title: &str,
    graph: &GraphSnapshot,
    kind: NodeKind,
    complete: bool,
) {
    let nodes: Vec<_> = graph
        .nodes
        .iter()
        .filter(|node| node.kind == kind && node.validity != Validity::Superseded)
        .collect();
    if nodes.is_empty() && complete {
        output.push_str(&format!("## {title}\n\n"));
        output.push_str("None recorded.\n\n");
        return;
    }
    if nodes.is_empty() {
        return;
    }
    output.push_str(&format!("## {title}\n\n"));
    for node in nodes {
        output.push_str(&format!("### {}\n\n", node.title));
        if !node.summary.is_empty() {
            output.push_str(&format!("{}\n\n", node.summary));
        }
        if !node.body.is_empty() {
            output.push_str(&format!("{}\n\n", node.body));
        }
        output.push_str(&format!(
            "- ID: `{}`\n- Status: {} / {}\n",
            node.id, node.lifecycle, node.validity
        ));
        if let Some(confidence) = node.confidence {
            output.push_str(&format!("- Confidence: {confidence}\n"));
        }
        if let Some(reason) = &node.confidence_reason {
            output.push_str(&format!("- Confidence reason: {reason}\n"));
        }
        output.push('\n');
    }
}

fn read_yaml_or_default<T>(path: &Path) -> Result<T, ExportError>
where
    T: serde::de::DeserializeOwned + Default,
{
    if path.exists() {
        Ok(serde_yaml::from_str(&fs::read_to_string(path)?)?)
    } else {
        Ok(T::default())
    }
}
