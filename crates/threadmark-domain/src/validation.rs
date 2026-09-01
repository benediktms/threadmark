use std::collections::{HashMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{DecisionPayload, Edge, EdgeType, GraphSnapshot, Lifecycle, Node, NodeKind, Validity};

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("node {0} was not found")]
    NodeNotFound(String),
    #[error("self-edges are forbidden")]
    SelfEdge,
    #[error("invalid {edge_type} edge from {source_kind} to {target_kind}: {reason}")]
    InvalidEdge {
        edge_type: EdgeType,
        source_kind: NodeKind,
        target_kind: NodeKind,
        reason: &'static str,
    },
    #[error("adding the edge would create a {0} cycle")]
    CycleDetected(EdgeType),
    #[error("invalid state: {0}")]
    InvalidState(String),
}

#[derive(Clone, Copy, Debug, Eq, JsonSchema, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LintSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct LintFinding {
    pub code: String,
    pub severity: LintSeverity,
    pub message: String,
    pub node_ids: Vec<String>,
}

pub fn validate_edge(source: &Node, edge_type: EdgeType, target: &Node) -> Result<(), DomainError> {
    if source.id == target.id {
        return Err(DomainError::SelfEdge);
    }

    let valid = match edge_type {
        EdgeType::Assumes => target.kind == NodeKind::Assumption,
        EdgeType::Produces => {
            matches!(source.kind, NodeKind::Experiment | NodeKind::Action)
                && matches!(target.kind, NodeKind::Evidence | NodeKind::Observation)
        }
        EdgeType::Resolves => target.kind == NodeKind::Question,
        _ => true,
    };

    if valid {
        Ok(())
    } else {
        Err(DomainError::InvalidEdge {
            edge_type,
            source_kind: source.kind,
            target_kind: target.kind,
            reason: "edge endpoint kinds do not satisfy the graph contract",
        })
    }
}

pub fn lint_graph(graph: &GraphSnapshot) -> Vec<LintFinding> {
    let nodes: HashMap<_, _> = graph.nodes.iter().map(|node| (node.id.as_str(), node)).collect();
    let mut findings = Vec::new();

    for edge in &graph.edges {
        let Some(source) = nodes.get(edge.source_node_id.as_str()) else {
            findings.push(error("TM001", format!("edge {} has a missing source", edge.id), vec![]));
            continue;
        };
        let Some(target) = nodes.get(edge.target_node_id.as_str()) else {
            findings.push(error("TM002", format!("edge {} has a missing target", edge.id), vec![]));
            continue;
        };
        if let Err(issue) = validate_edge(source, edge.edge_type, target) {
            findings.push(error(
                "TM003",
                issue.to_string(),
                vec![source.id.clone(), target.id.clone()],
            ));
        }
    }

    for edge_type in [EdgeType::Requires, EdgeType::Supersedes] {
        if let Some(cycle) = find_cycle(&graph.nodes, &graph.edges, edge_type) {
            findings.push(error(
                "TM004",
                format!("{} edge cycle detected", edge_type.as_str()),
                cycle,
            ));
        }
    }

    for node in &graph.nodes {
        if node.confidence.is_some()
            && node
                .confidence_reason
                .as_deref()
                .is_none_or(str::is_empty)
        {
            findings.push(error(
                "TM005",
                format!("{} has confidence without a reason", node.title),
                vec![node.id.clone()],
            ));
        }
        if node.kind == NodeKind::Decision && node.lifecycle == Lifecycle::Resolved {
            match serde_json::from_value::<DecisionPayload>(node.payload.clone()) {
                Ok(payload) => {
                    let selected = payload
                        .alternatives
                        .iter()
                        .filter(|alternative| {
                            alternative.status == crate::AlternativeStatus::Selected
                        })
                        .count();
                    if selected != 1 || payload.selected_option.is_none() {
                        findings.push(error(
                            "TM006",
                            format!("resolved decision {} must select exactly one alternative", node.title),
                            vec![node.id.clone()],
                        ));
                    }
                }
                Err(_) => findings.push(error(
                    "TM007",
                    format!("decision {} has an invalid payload", node.title),
                    vec![node.id.clone()],
                )),
            }
        }
        if node.kind == NodeKind::Question
            && node.lifecycle == Lifecycle::Resolved
            && node.body.trim().is_empty()
            && !graph.edges.iter().any(|edge| {
                edge.edge_type == EdgeType::Resolves && edge.target_node_id == node.id
            })
        {
            findings.push(error(
                "TM008",
                format!("resolved question {} has no answer or resolving node", node.title),
                vec![node.id.clone()],
            ));
        }
        if node.kind == NodeKind::Evidence
            && !graph.node_source_ids.contains_key(node.id.as_str())
        {
            findings.push(LintFinding {
                code: "TM009".into(),
                severity: LintSeverity::Warning,
                message: format!("evidence {} has no provenance", node.title),
                node_ids: vec![node.id.clone()],
            });
        }
        if node.validity == Validity::Invalid
            && graph.edges.iter().any(|edge| {
                edge.edge_type == EdgeType::Assumes
                    && edge.target_node_id == node.id
                    && nodes
                        .get(edge.source_node_id.as_str())
                        .is_some_and(|dependent| dependent.validity == Validity::Current)
            })
        {
            findings.push(error(
                "TM010",
                format!("invalid assumption {} still has current direct dependents", node.title),
                vec![node.id.clone()],
            ));
        }
    }

    for fog in &graph.fog_patches {
        if fog.status == crate::FogStatus::Graduated && fog.graduated_to.is_empty() {
            findings.push(error(
                "TM011",
                format!("graduated fog patch {} has no target nodes", fog.title),
                vec![],
            ));
        }
    }

    findings
}

fn error(code: &str, message: String, node_ids: Vec<String>) -> LintFinding {
    LintFinding {
        code: code.into(),
        severity: LintSeverity::Error,
        message,
        node_ids,
    }
}

fn find_cycle(nodes: &[Node], edges: &[Edge], edge_type: EdgeType) -> Option<Vec<String>> {
    let mut adjacency: HashMap<&str, Vec<&str>> = HashMap::new();
    for edge in edges.iter().filter(|edge| edge.edge_type == edge_type) {
        adjacency
            .entry(edge.source_node_id.as_str())
            .or_default()
            .push(edge.target_node_id.as_str());
    }

    fn visit<'a>(
        node: &'a str,
        adjacency: &HashMap<&'a str, Vec<&'a str>>,
        visiting: &mut HashSet<&'a str>,
        visited: &mut HashSet<&'a str>,
        path: &mut Vec<&'a str>,
    ) -> Option<Vec<String>> {
        if visiting.contains(node) {
            let start = path.iter().position(|entry| *entry == node).unwrap_or(0);
            return Some(path[start..].iter().map(|entry| (*entry).to_owned()).collect());
        }
        if visited.contains(node) {
            return None;
        }
        visiting.insert(node);
        path.push(node);
        for target in adjacency.get(node).into_iter().flatten() {
            if let Some(cycle) = visit(target, adjacency, visiting, visited, path) {
                return Some(cycle);
            }
        }
        path.pop();
        visiting.remove(node);
        visited.insert(node);
        None
    }

    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    for node in nodes {
        let mut path = Vec::new();
        if let Some(cycle) = visit(
            node.id.as_str(),
            &adjacency,
            &mut visiting,
            &mut visited,
            &mut path,
        ) {
            return Some(cycle);
        }
    }
    None
}
