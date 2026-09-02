use std::{fmt, str::FromStr};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type Id = String;

macro_rules! string_enum {
    ($name:ident { $($variant:ident => $value:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, JsonSchema, PartialEq, Serialize, Deserialize)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($variant),+ }

        impl $name {
            #[must_use]
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $value),+ }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = String;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant),)+
                    other => Err(format!("invalid {}: {other}", stringify!($name))),
                }
            }
        }
    };
}

string_enum!(EffortStatus {
    Active => "active",
    Ready => "ready",
    Completed => "completed",
    Abandoned => "abandoned",
});

string_enum!(NodeKind {
    Destination => "destination",
    Question => "question",
    Decision => "decision",
    Assumption => "assumption",
    Evidence => "evidence",
    Experiment => "experiment",
    Observation => "observation",
    Constraint => "constraint",
    Action => "action",
});

string_enum!(Lifecycle {
    Draft => "draft",
    Open => "open",
    InProgress => "in_progress",
    Resolved => "resolved",
    OutOfScope => "out_of_scope",
    Archived => "archived",
});

string_enum!(Validity {
    Current => "current",
    Challenged => "challenged",
    Undermined => "undermined",
    ReviewRequired => "review_required",
    Invalid => "invalid",
    Superseded => "superseded",
    Stale => "stale",
});

string_enum!(Confidence {
    Tentative => "tentative",
    Supported => "supported",
    Strong => "strong",
});

impl Confidence {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Tentative => 1,
            Self::Supported => 2,
            Self::Strong => 3,
        }
    }
}

string_enum!(Reversibility {
    Easy => "easy",
    Moderate => "moderate",
    Expensive => "expensive",
});

string_enum!(RiskLevel {
    Low => "low",
    Medium => "medium",
    High => "high",
    Critical => "critical",
});

impl RiskLevel {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Low => 1,
            Self::Medium => 2,
            Self::High => 3,
            Self::Critical => 4,
        }
    }
}

string_enum!(Uncertainty {
    Low => "low",
    Medium => "medium",
    High => "high",
});

impl Uncertainty {
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::Low => 1,
            Self::Medium => 2,
            Self::High => 3,
        }
    }
}

string_enum!(EdgeType {
    Requires => "requires",
    Informs => "informs",
    Supports => "supports",
    Contradicts => "contradicts",
    Assumes => "assumes",
    Produces => "produces",
    Resolves => "resolves",
    Supersedes => "supersedes",
});

string_enum!(FogStatus {
    Active => "active",
    Graduated => "graduated",
    OutOfScope => "out_of_scope",
});

string_enum!(FindingStatus {
    Proposed => "proposed",
    Accepted => "accepted",
    Rejected => "rejected",
    Resolved => "resolved",
});

string_enum!(FindingType {
    Contradiction => "contradiction",
    SupportGap => "support_gap",
    Lint => "lint",
});

string_enum!(SourceKind {
    Url => "url",
    File => "file",
    GitCommit => "git_commit",
    PullRequest => "pull_request",
    Issue => "issue",
    Benchmark => "benchmark",
    CommandOutput => "command_output",
    Conversation => "conversation",
    Person => "person",
    Other => "other",
});

string_enum!(SourceTrust {
    Unreviewed => "unreviewed",
    Reviewed => "reviewed",
    Authoritative => "authoritative",
});

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Workspace {
    pub id: Id,
    pub name: String,
    pub root_uri: String,
    pub schema_version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Effort {
    pub id: Id,
    pub workspace_id: Id,
    pub slug: String,
    pub title: String,
    pub destination: String,
    pub scope_notes: String,
    pub status: EffortStatus,
    pub version: i64,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: Id,
    pub effort_id: Id,
    pub kind: NodeKind,
    pub title: String,
    pub summary: String,
    pub lifecycle: Lifecycle,
    pub validity: Validity,
    pub confidence: Option<Confidence>,
    pub confidence_reason: Option<String>,
    pub reversibility: Option<Reversibility>,
    pub impact: Option<RiskLevel>,
    pub uncertainty: Option<Uncertainty>,
    pub cost_of_wrong: Option<RiskLevel>,
    pub current_revision: i64,
    pub body: String,
    pub payload: Value,
    pub created_at: String,
    pub updated_at: String,
}

impl Node {
    #[must_use]
    pub const fn claimable(&self) -> bool {
        matches!(
            self.kind,
            NodeKind::Question | NodeKind::Decision | NodeKind::Experiment | NodeKind::Action
        )
    }

    #[must_use]
    pub const fn usable(&self) -> bool {
        !matches!(
            self.validity,
            Validity::Invalid
                | Validity::Undermined
                | Validity::ReviewRequired
                | Validity::Superseded
                | Validity::Stale
        )
    }
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct NodeRevision {
    pub node_id: Id,
    pub revision: i64,
    pub body: String,
    pub payload: Value,
    pub reason: Option<String>,
    pub actor_id: String,
    pub session_id: Option<String>,
    pub created_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Edge {
    pub id: Id,
    pub effort_id: Id,
    pub source_node_id: Id,
    pub edge_type: EdgeType,
    pub target_node_id: Id,
    pub rationale: Option<String>,
    pub created_by: String,
    pub created_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub id: Id,
    pub node_id: Id,
    pub actor_id: String,
    pub claimant: String,
    pub claimed_at: String,
    pub heartbeat_at: String,
    pub lease_expires_at: String,
    pub released_at: Option<String>,
    pub release_reason: Option<String>,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct FogPatch {
    pub id: Id,
    pub effort_id: Id,
    pub title: String,
    pub description: String,
    pub anchor_node_id: Option<Id>,
    pub status: FogStatus,
    pub graduated_to: Vec<Id>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Finding {
    pub id: Id,
    pub effort_id: Id,
    pub finding_type: FindingType,
    pub severity: RiskLevel,
    pub status: FindingStatus,
    pub title: String,
    pub detail: String,
    pub related_nodes: Vec<Id>,
    pub proposed_by: Option<String>,
    pub adjudication: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub id: Id,
    pub effort_id: Id,
    pub kind: SourceKind,
    pub uri: Option<String>,
    pub title: String,
    pub retrieved_at: Option<String>,
    pub observed_at: Option<String>,
    pub content_hash: Option<String>,
    pub excerpt: Option<String>,
    pub metadata: Value,
    pub trust: SourceTrust,
    pub created_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct ExitCriterion {
    pub id: Id,
    pub effort_id: Id,
    pub criterion_type: String,
    pub config: Value,
    pub required: bool,
    pub created_at: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Id,
    pub effort_id: Option<Id>,
    pub actor_id: String,
    #[schemars(skip)]
    #[serde(skip_serializing)]
    pub session_id: Option<String>,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: Id,
    pub before: Option<Value>,
    pub after: Option<Value>,
    pub reason: Option<String>,
    pub occurred_at: String,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct EventFilter {
    pub effort_id: Option<Id>,
    pub entity_type: Option<String>,
    pub entity_id: Option<Id>,
    pub actor_id: Option<String>,
    pub event_type: Option<String>,
    pub occurred_from: Option<String>,
    pub occurred_to: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::AuditEvent;

    #[test]
    fn audit_event_hides_sessions_on_write_without_losing_them_on_read() {
        let event: AuditEvent = serde_json::from_value(json!({
            "id": "event",
            "effort_id": "effort",
            "actor_id": "actor",
            "session_id": "session",
            "event_type": "node_created",
            "entity_type": "node",
            "entity_id": "node",
            "before": null,
            "after": null,
            "reason": null,
            "occurred_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();

        assert_eq!(event.session_id.as_deref(), Some("session"));
        assert!(
            serde_json::to_value(event)
                .unwrap()
                .get("session_id")
                .is_none()
        );
    }
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct Alternative {
    pub id: String,
    pub label: String,
    pub status: AlternativeStatus,
    pub reason: String,
}

string_enum!(AlternativeStatus {
    Proposed => "proposed",
    Selected => "selected",
    Rejected => "rejected",
});

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct DecisionPayload {
    pub prompt: String,
    pub alternatives: Vec<Alternative>,
    pub selected_option: Option<String>,
    pub rationale: String,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct NewNode {
    pub kind: NodeKind,
    pub title: String,
    pub summary: String,
    pub body: String,
    pub payload: Value,
    pub lifecycle: Lifecycle,
    pub confidence: Option<Confidence>,
    pub confidence_reason: Option<String>,
    pub reversibility: Option<Reversibility>,
    pub impact: Option<RiskLevel>,
    pub uncertainty: Option<Uncertainty>,
    pub cost_of_wrong: Option<RiskLevel>,
}

#[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
pub struct NewEdge {
    pub source_node_id: Id,
    pub edge_type: EdgeType,
    pub target_node_id: Id,
    pub rationale: Option<String>,
}
