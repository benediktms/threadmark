//! Domain entities and deterministic graph semantics for Threadmark.

mod graph;
mod model;
mod validation;

pub use graph::{
    FrontierEntry, GraphSnapshot, InvalidationChange, InvalidationPreview, ReadinessReport,
    RequirementResult, calculate_frontier, evaluate_readiness, preview_invalidation,
};
pub use model::*;
pub use validation::{DomainError, LintFinding, LintSeverity, lint_graph, validate_edge};
