use std::collections::{HashMap, HashSet, VecDeque};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Claim, Confidence, Edge, EdgeType, ExitCriterion, Finding, FindingStatus, FogPatch, FogStatus,
    Id, Lifecycle, LintSeverity, Node, NodeKind, Reversibility, RiskLevel, Uncertainty, Validity,
    lint_graph,
};

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct GraphSnapshot {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub claims: Vec<Claim>,
    pub fog_patches: Vec<FogPatch>,
    pub findings: Vec<Finding>,
    pub exit_criteria: Vec<ExitCriterion>,
    #[serde(default)]
    pub node_source_ids: HashMap<Id, Vec<Id>>,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct FrontierEntry {
    pub node: Node,
    pub cost_of_wrong_rank: u8,
    pub impact_rank: u8,
    pub uncertainty_rank: u8,
    pub downstream_fanout: usize,
    pub explanation: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InvalidationChange {
    pub node_id: Id,
    pub from: Validity,
    pub to: Validity,
    pub reason: String,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct InvalidationPreview {
    pub changes: Vec<InvalidationChange>,
    pub reopened_questions: Vec<Id>,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct RequirementResult {
    pub criterion_id: Id,
    pub criterion_type: String,
    pub required: bool,
    pub passed: bool,
    pub explanation: String,
    pub related_nodes: Vec<Id>,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ReadinessReport {
    pub ready: bool,
    pub results: Vec<RequirementResult>,
}

pub fn calculate_frontier(graph: &GraphSnapshot, now: &str) -> Vec<FrontierEntry> {
    let nodes: HashMap<_, _> = graph
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let claimed: HashSet<_> = graph
        .claims
        .iter()
        .filter(|claim| claim.released_at.is_none() && claim.lease_expires_at.as_str() > now)
        .map(|claim| claim.node_id.as_str())
        .collect();

    let mut entries: Vec<_> = graph
        .nodes
        .iter()
        .filter(|node| node.claimable())
        .filter(|node| node.lifecycle == Lifecycle::Open)
        .filter(|node| node.usable())
        .filter(|node| !claimed.contains(node.id.as_str()))
        .filter(|node| {
            graph
                .edges
                .iter()
                .filter(|edge| {
                    edge.source_node_id == node.id && edge.edge_type == EdgeType::Requires
                })
                .all(|edge| {
                    nodes
                        .get(edge.target_node_id.as_str())
                        .is_some_and(|required| {
                            required.lifecycle == Lifecycle::Resolved && required.usable()
                        })
                })
        })
        .map(|node| {
            let cost = node.cost_of_wrong.unwrap_or(RiskLevel::Medium).rank();
            let impact = node.impact.unwrap_or(RiskLevel::Medium).rank();
            let uncertainty = node.uncertainty.unwrap_or(Uncertainty::High).rank();
            let fanout = downstream_fanout(&node.id, &graph.edges);
            FrontierEntry {
                node: node.clone(),
                cost_of_wrong_rank: cost,
                impact_rank: impact,
                uncertainty_rank: uncertainty,
                downstream_fanout: fanout,
                explanation: format!(
                    "cost={}, impact={}, uncertainty={}, downstream={fanout}",
                    node.cost_of_wrong.map_or("unknown", RiskLevel::as_str),
                    node.impact.map_or("unknown", RiskLevel::as_str),
                    node.uncertainty.map_or("unknown", Uncertainty::as_str),
                ),
            }
        })
        .collect();

    entries.sort_by(|left, right| {
        right
            .cost_of_wrong_rank
            .cmp(&left.cost_of_wrong_rank)
            .then_with(|| right.impact_rank.cmp(&left.impact_rank))
            .then_with(|| right.uncertainty_rank.cmp(&left.uncertainty_rank))
            .then_with(|| right.downstream_fanout.cmp(&left.downstream_fanout))
            .then_with(|| left.node.created_at.cmp(&right.node.created_at))
            .then_with(|| left.node.id.cmp(&right.node.id))
    });
    entries
}

fn downstream_fanout(node_id: &str, edges: &[Edge]) -> usize {
    let mut seen = HashSet::new();
    let mut queue = VecDeque::from([node_id]);
    while let Some(current) = queue.pop_front() {
        for edge in edges
            .iter()
            .filter(|edge| edge.edge_type == EdgeType::Requires && edge.target_node_id == current)
        {
            if seen.insert(edge.source_node_id.as_str()) {
                queue.push_back(edge.source_node_id.as_str());
            }
        }
    }
    seen.len()
}

pub fn preview_invalidation(
    graph: &GraphSnapshot,
    node_id: &str,
    target_validity: Validity,
) -> InvalidationPreview {
    let nodes: HashMap<_, _> = graph
        .nodes
        .iter()
        .map(|node| (node.id.as_str(), node))
        .collect();
    let Some(root) = nodes.get(node_id) else {
        return InvalidationPreview::default();
    };

    let mut preview = InvalidationPreview::default();
    preview.changes.push(InvalidationChange {
        node_id: root.id.clone(),
        from: root.validity,
        to: target_validity,
        reason: "explicit state change".into(),
    });

    let mut affected = HashSet::from([node_id]);
    let mut queue = VecDeque::from([node_id]);
    while let Some(current) = queue.pop_front() {
        for edge in &graph.edges {
            let direct_assumption = edge.edge_type == EdgeType::Assumes
                && edge.target_node_id == current
                && target_validity == Validity::Invalid;
            let required = edge.edge_type == EdgeType::Requires && edge.target_node_id == current;
            if !direct_assumption && !required {
                continue;
            }
            let dependent_id = edge.source_node_id.as_str();
            if !affected.insert(dependent_id) {
                continue;
            }
            if let Some(dependent) = nodes.get(dependent_id) {
                let to = if direct_assumption {
                    Validity::Undermined
                } else {
                    Validity::ReviewRequired
                };
                if dependent.validity != to {
                    preview.changes.push(InvalidationChange {
                        node_id: dependent.id.clone(),
                        from: dependent.validity,
                        to,
                        reason: format!("{} depends on {current}", dependent.title),
                    });
                }
                queue.push_back(dependent_id);
            }
        }
    }

    for edge in graph.edges.iter().filter(|edge| {
        edge.edge_type == EdgeType::Resolves && affected.contains(edge.source_node_id.as_str())
    }) {
        let another_resolution = graph.edges.iter().any(|candidate| {
            candidate.edge_type == EdgeType::Resolves
                && candidate.target_node_id == edge.target_node_id
                && !affected.contains(candidate.source_node_id.as_str())
                && nodes
                    .get(candidate.source_node_id.as_str())
                    .is_some_and(|node| node.usable())
        });
        if !another_resolution {
            preview.reopened_questions.push(edge.target_node_id.clone());
        }
    }
    preview
}

#[allow(clippy::too_many_lines)]
pub fn evaluate_readiness(graph: &GraphSnapshot) -> ReadinessReport {
    let mut results = Vec::new();
    let criteria = if graph.exit_criteria.is_empty() {
        vec![
            synthetic("no_open_required_nodes"),
            synthetic("no_active_fog"),
            synthetic("no_undermined_decisions"),
            synthetic("no_review_required_decisions"),
            synthetic("no_blocking_findings"),
        ]
    } else {
        graph.exit_criteria.clone()
    };

    for criterion in criteria {
        let (passed, explanation, related_nodes) = match criterion.criterion_type.as_str() {
            "no_open_required_nodes" => {
                let nodes: Vec<_> = graph
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.claimable()
                            && matches!(node.lifecycle, Lifecycle::Open | Lifecycle::InProgress)
                            && node.validity != Validity::Superseded
                    })
                    .map(|node| node.id.clone())
                    .collect();
                (
                    nodes.is_empty(),
                    format!("{} open required nodes", nodes.len()),
                    nodes,
                )
            }
            "no_active_fog" => {
                let count = graph
                    .fog_patches
                    .iter()
                    .filter(|fog| fog.status == FogStatus::Active)
                    .count();
                (count == 0, format!("{count} active fog patches"), vec![])
            }
            "no_undermined_decisions" => validity_check(graph, Validity::Undermined),
            "no_review_required_decisions" => validity_check(graph, Validity::ReviewRequired),
            "no_blocking_findings" => {
                let ids: Vec<_> = graph
                    .findings
                    .iter()
                    .filter(|finding| {
                        matches!(
                            finding.status,
                            FindingStatus::Proposed | FindingStatus::Accepted
                        ) && matches!(finding.severity, RiskLevel::High | RiskLevel::Critical)
                    })
                    .flat_map(|finding| finding.related_nodes.clone())
                    .collect();
                (
                    ids.is_empty(),
                    format!("{} blocking findings", ids.len()),
                    ids,
                )
            }
            "requires_confidence_for_reversibility" => {
                let required = criterion
                    .config
                    .get("expensive")
                    .and_then(|value| value.as_str())
                    .and_then(|value| value.parse::<Confidence>().ok())
                    .unwrap_or(Confidence::Supported);
                let ids: Vec<_> = graph
                    .nodes
                    .iter()
                    .filter(|node| {
                        node.kind == NodeKind::Decision
                            && node.lifecycle == Lifecycle::Resolved
                            && node.reversibility == Some(Reversibility::Expensive)
                            && node
                                .confidence
                                .is_none_or(|confidence| confidence.rank() < required.rank())
                    })
                    .map(|node| node.id.clone())
                    .collect();
                (
                    ids.is_empty(),
                    format!(
                        "{} expensive decisions below {required} confidence",
                        ids.len()
                    ),
                    ids,
                )
            }
            "node_resolved" | "node_valid" => {
                let id = criterion
                    .config
                    .get("node")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default();
                let node = graph.nodes.iter().find(|node| node.id == id);
                let passed = node.is_some_and(|node| {
                    if criterion.criterion_type == "node_resolved" {
                        node.lifecycle == Lifecycle::Resolved
                    } else {
                        node.usable()
                    }
                });
                (
                    passed,
                    format!("node {id} satisfies {}: {passed}", criterion.criterion_type),
                    vec![id.into()],
                )
            }
            other => (
                false,
                format!("unsupported readiness criterion: {other}"),
                vec![],
            ),
        };
        results.push(RequirementResult {
            criterion_id: criterion.id,
            criterion_type: criterion.criterion_type,
            required: criterion.required,
            passed,
            explanation,
            related_nodes,
        });
    }

    let lint_errors = lint_graph(graph)
        .into_iter()
        .filter(|finding| finding.severity == LintSeverity::Error)
        .count();
    results.push(RequirementResult {
        criterion_id: "builtin-lint".into(),
        criterion_type: "lint_clean".into(),
        required: true,
        passed: lint_errors == 0,
        explanation: format!("{lint_errors} lint errors"),
        related_nodes: vec![],
    });

    let ready = results
        .iter()
        .all(|result| !result.required || result.passed);
    ReadinessReport { ready, results }
}

fn synthetic(criterion_type: &str) -> ExitCriterion {
    ExitCriterion {
        id: format!("builtin-{criterion_type}"),
        effort_id: String::new(),
        criterion_type: criterion_type.into(),
        config: serde_json::json!({}),
        required: true,
        created_at: String::new(),
    }
}

fn validity_check(graph: &GraphSnapshot, validity: Validity) -> (bool, String, Vec<String>) {
    let ids: Vec<_> = graph
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Decision && node.validity == validity)
        .map(|node| node.id.clone())
        .collect();
    (
        ids.is_empty(),
        format!("{} decisions are {validity}", ids.len()),
        ids,
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    fn node(id: &str, kind: NodeKind, lifecycle: Lifecycle) -> Node {
        Node {
            id: id.into(),
            effort_id: "effort".into(),
            kind,
            title: id.into(),
            summary: String::new(),
            lifecycle,
            validity: Validity::Current,
            confidence: None,
            confidence_reason: None,
            reversibility: None,
            impact: Some(RiskLevel::Medium),
            uncertainty: Some(Uncertainty::Medium),
            cost_of_wrong: Some(RiskLevel::Medium),
            current_revision: 1,
            body: String::new(),
            payload: json!({}),
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    fn edge(source: &str, edge_type: EdgeType, target: &str) -> Edge {
        Edge {
            id: format!("{source}-{target}"),
            effort_id: "effort".into(),
            source_node_id: source.into(),
            edge_type,
            target_node_id: target.into(),
            rationale: None,
            created_by: "test".into(),
            created_at: "2026-01-01T00:00:00Z".into(),
        }
    }

    #[test]
    fn blocked_node_is_not_on_frontier() {
        let graph = GraphSnapshot {
            nodes: vec![
                node("question", NodeKind::Question, Lifecycle::Open),
                node("research", NodeKind::Question, Lifecycle::Open),
            ],
            edges: vec![edge("question", EdgeType::Requires, "research")],
            ..GraphSnapshot::default()
        };
        let frontier = calculate_frontier(&graph, "2026-01-01T00:00:00Z");
        assert_eq!(frontier.len(), 1);
        assert_eq!(frontier[0].node.id, "research");
    }

    #[test]
    fn invalid_assumption_undermines_direct_decision_and_reviews_dependents() {
        let graph = GraphSnapshot {
            nodes: vec![
                node("assumption", NodeKind::Assumption, Lifecycle::Resolved),
                node("decision", NodeKind::Decision, Lifecycle::Resolved),
                node("dependent", NodeKind::Decision, Lifecycle::Resolved),
            ],
            edges: vec![
                edge("decision", EdgeType::Assumes, "assumption"),
                edge("dependent", EdgeType::Requires, "decision"),
            ],
            ..GraphSnapshot::default()
        };
        let preview = preview_invalidation(&graph, "assumption", Validity::Invalid);
        assert_eq!(preview.changes.len(), 3);
        assert!(
            preview.changes.iter().any(|change| {
                change.node_id == "decision" && change.to == Validity::Undermined
            })
        );
        assert!(preview.changes.iter().any(|change| {
            change.node_id == "dependent" && change.to == Validity::ReviewRequired
        }));
    }
}
