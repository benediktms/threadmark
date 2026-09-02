//! Transactional application workflows shared by the CLI and MCP adapters.

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use thiserror::Error;
use threadmark_domain::{
    AuditEvent, Claim, Confidence, DomainError, Edge, Effort, EffortStatus, EventFilter,
    ExitCriterion, Finding, FindingStatus, FindingType, FogPatch, FogStatus, FrontierEntry,
    GraphSnapshot, InvalidationPreview, Lifecycle, LintFinding, NewEdge, NewNode, Node,
    NodeRevision, ReadinessReport, RiskLevel, Source, SourceKind, SourceTrust, Validity, Workspace,
    calculate_frontier, evaluate_readiness, lint_graph, preview_invalidation, validate_edge,
};
use threadmark_store::{ClaimGuard, Store, StoreError};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};
use ulid::Ulid;

const SCHEMA_VERSION: i64 = 1;
const MARKER: &str = ".threadmark/workspace.toml";

#[derive(Debug, Error)]
pub enum ApplicationError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Threadmark is not initialized below {0}")]
    NotInitialized(String),
    #[error("invalid workspace marker: {0}")]
    InvalidMarker(String),
    #[error("node {0} is not on the actionable frontier")]
    NotOnFrontier(String),
    #[error("graph mutation would violate invariants: {0}")]
    InvalidGraph(String),
    #[error("effort is not ready: {0}")]
    EffortNotReady(String),
    #[error("effort is not active: {0}")]
    EffortNotActive(EffortStatus),
    #[error("expected version {expected}, but effort is at version {actual}")]
    VersionConflict { expected: i64, actual: i64 },
}

#[derive(Clone, Debug)]
pub struct Service {
    root: PathBuf,
    workspace: Workspace,
    store: Store,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct EffortStatusView {
    pub effort: Effort,
    pub frontier: Vec<FrontierEntry>,
    pub readiness: ReadinessReport,
    pub lint: Vec<LintFinding>,
    pub active_fog: usize,
    pub active_findings: usize,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct CreateEffort {
    pub slug: String,
    pub title: String,
    pub destination: String,
    pub scope_notes: String,
    pub actor_id: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct AddNode {
    pub effort: String,
    pub node: NewNode,
    pub actor_id: String,
    pub session_id: Option<String>,
    pub expected_version: Option<i64>,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
pub struct AddEdge {
    pub effort: String,
    pub edge: NewEdge,
    pub actor_id: String,
    pub expected_version: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct WorkspaceMarker {
    schema_version: i64,
    workspace_id: String,
    name: String,
}

impl Service {
    pub async fn init(root: &Path, name: &str) -> Result<Self, ApplicationError> {
        let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        let marker_path = root.join(MARKER);
        if marker_path.exists() {
            return Self::open(&root).await;
        }
        fs::create_dir_all(root.join(".threadmark/exports"))?;
        let workspace_id = id();
        let marker = toml::to_string(&WorkspaceMarker {
            schema_version: SCHEMA_VERSION,
            workspace_id: workspace_id.clone(),
            name: name.into(),
        })
        .map_err(|error| ApplicationError::InvalidMarker(error.to_string()))?;
        fs::write(&marker_path, marker)?;
        let timestamp = now();
        let workspace = Workspace {
            id: workspace_id,
            name: name.into(),
            root_uri: root.to_string_lossy().into_owned(),
            schema_version: SCHEMA_VERSION,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        let database_path = database_path(&root);
        let store = Store::connect(&database_path).await?;
        store.create_workspace(&workspace).await?;
        Ok(Self {
            root,
            workspace,
            store,
        })
    }

    pub async fn open(start: &Path) -> Result<Self, ApplicationError> {
        let root = discover_root(start)?;
        let marker: WorkspaceMarker = toml::from_str(&fs::read_to_string(root.join(MARKER))?)
            .map_err(|error| ApplicationError::InvalidMarker(error.to_string()))?;
        if marker.schema_version > SCHEMA_VERSION {
            return Err(ApplicationError::InvalidMarker(format!(
                "schema version {} is newer than supported {SCHEMA_VERSION}",
                marker.schema_version
            )));
        }
        let timestamp = now();
        let workspace = Workspace {
            id: marker.workspace_id.clone(),
            name: marker.name,
            root_uri: root.to_string_lossy().into_owned(),
            schema_version: marker.schema_version,
            created_at: timestamp.clone(),
            updated_at: timestamp,
        };
        let database_path = database_path(&root);
        let store = Store::connect(&database_path).await?;
        store.reconcile_workspace(&workspace).await?;
        let workspace = store.get_workspace(&marker.workspace_id).await?;
        Ok(Self {
            root,
            workspace,
            store,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn workspace(&self) -> &Workspace {
        &self.workspace
    }

    pub async fn create_effort(&self, input: CreateEffort) -> Result<Effort, ApplicationError> {
        let timestamp = now();
        let effort = Effort {
            id: id(),
            workspace_id: self.workspace.id.clone(),
            slug: input.slug,
            title: input.title,
            destination: input.destination,
            scope_notes: input.scope_notes,
            status: EffortStatus::Active,
            version: 1,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
        };
        let event = event(
            Some(&effort.id),
            &input.actor_id,
            None,
            "effort_created",
            "effort",
            &effort.id,
            None,
            Some(serde_json::to_value(&effort)?),
            None,
            &timestamp,
        );
        self.store.create_effort(&effort, &event).await?;
        Ok(effort)
    }

    pub async fn list_efforts(&self) -> Result<Vec<Effort>, ApplicationError> {
        Ok(self.store.list_efforts(&self.workspace.id).await?)
    }

    pub async fn get_effort(&self, selector: &str) -> Result<Effort, ApplicationError> {
        Ok(self.store.get_effort(&self.workspace.id, selector).await?)
    }

    async fn active_effort(&self, selector: &str) -> Result<Effort, ApplicationError> {
        let effort = self.get_effort(selector).await?;
        if effort.status != EffortStatus::Active {
            return Err(ApplicationError::EffortNotActive(effort.status));
        }
        Ok(effort)
    }

    pub async fn complete_effort(
        &self,
        selector: &str,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<Effort, ApplicationError> {
        let status = self.status(selector).await?;
        self.active_effort(selector).await?;
        if !status.readiness.ready {
            return Err(ApplicationError::EffortNotReady(
                status
                    .readiness
                    .results
                    .into_iter()
                    .filter(|result| !result.passed)
                    .map(|result| result.explanation)
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }
        let expected = expected_version.unwrap_or(status.effort.version);
        let timestamp = now();
        let mut effort = status.effort.clone();
        effort.status = EffortStatus::Completed;
        effort.version += 1;
        effort.updated_at = timestamp.clone();
        let event = event(
            Some(&effort.id),
            actor_id,
            None,
            "effort_completed",
            "effort",
            &effort.id,
            Some(serde_json::to_value(&status.effort)?),
            Some(serde_json::to_value(&effort)?),
            None,
            &timestamp,
        );
        effort.version = self
            .store
            .complete_effort(&effort, &event, expected)
            .await?;
        Ok(effort)
    }

    pub async fn add_node(&self, input: AddNode) -> Result<(Node, i64), ApplicationError> {
        let effort = self.active_effort(&input.effort).await?;
        let expected = input.expected_version.unwrap_or(effort.version);
        let timestamp = now();
        let node = Node {
            id: id(),
            effort_id: effort.id.clone(),
            kind: input.node.kind,
            title: input.node.title,
            summary: input.node.summary,
            lifecycle: input.node.lifecycle,
            validity: Validity::Current,
            confidence: input.node.confidence,
            confidence_reason: input.node.confidence_reason,
            reversibility: input.node.reversibility,
            impact: input.node.impact,
            uncertainty: input.node.uncertainty,
            cost_of_wrong: input.node.cost_of_wrong,
            current_revision: 1,
            body: input.node.body,
            payload: input.node.payload,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
        };
        validate_node(&node)?;
        let revision = revision(
            &node,
            &input.actor_id,
            input.session_id.as_deref(),
            "initial revision",
            &timestamp,
        );
        let event = event(
            Some(&effort.id),
            &input.actor_id,
            input.session_id.as_deref(),
            "node_created",
            "node",
            &node.id,
            None,
            Some(serde_json::to_value(&node)?),
            None,
            &timestamp,
        );
        let version = self
            .store
            .insert_node(&node, &revision, &event, expected)
            .await?;
        Ok((node, version))
    }

    pub async fn add_edge(&self, input: AddEdge) -> Result<(Edge, i64), ApplicationError> {
        let effort = self.active_effort(&input.effort).await?;
        let expected = input.expected_version.unwrap_or(effort.version);
        let source = self
            .store
            .get_node(&effort.id, &input.edge.source_node_id)
            .await?;
        let target = self
            .store
            .get_node(&effort.id, &input.edge.target_node_id)
            .await?;
        validate_edge(&source, input.edge.edge_type, &target)?;
        let timestamp = now();
        let edge = Edge {
            id: id(),
            effort_id: effort.id.clone(),
            source_node_id: source.id,
            edge_type: input.edge.edge_type,
            target_node_id: target.id,
            rationale: input.edge.rationale,
            created_by: input.actor_id.clone(),
            created_at: timestamp.clone(),
        };
        let mut prospective = self.store.snapshot(&effort.id).await?;
        prospective.edges.push(edge.clone());
        if let Some(issue) = lint_graph(&prospective)
            .into_iter()
            .find(|finding| finding.code == "TM004")
        {
            return Err(ApplicationError::InvalidGraph(issue.message));
        }
        let event = event(
            Some(&effort.id),
            &input.actor_id,
            None,
            "edge_created",
            "edge",
            &edge.id,
            None,
            Some(serde_json::to_value(&edge)?),
            None,
            &timestamp,
        );
        let version = self.store.insert_edge(&edge, &event, expected).await?;
        Ok((edge, version))
    }

    pub async fn snapshot(
        &self,
        effort: &str,
    ) -> Result<(Effort, GraphSnapshot), ApplicationError> {
        let effort = self.get_effort(effort).await?;
        if effort.status == EffortStatus::Active {
            self.store.reap_expired_claims(&effort.id).await?;
        }
        let snapshot = self.store.snapshot(&effort.id).await?;
        Ok((effort, snapshot))
    }

    pub async fn export_snapshot(
        &self,
        effort: &str,
    ) -> Result<
        (
            Effort,
            GraphSnapshot,
            Vec<threadmark_domain::Source>,
            Vec<AuditEvent>,
        ),
        ApplicationError,
    > {
        let effort = self.get_effort(effort).await?;
        Ok(self.store.snapshot_with_events(&effort.id).await?)
    }

    pub async fn status(&self, effort: &str) -> Result<EffortStatusView, ApplicationError> {
        let effort = self.get_effort(effort).await?;
        self.store.reap_expired_claims(&effort.id).await?;
        let graph = self.store.snapshot(&effort.id).await?;
        let frontier = calculate_frontier(&graph, &now());
        let readiness = evaluate_readiness(&graph);
        let lint = lint_graph(&graph);
        let active_fog = graph
            .fog_patches
            .iter()
            .filter(|fog| fog.status == FogStatus::Active)
            .count();
        let active_findings = graph
            .findings
            .iter()
            .filter(|finding| {
                matches!(
                    finding.status,
                    threadmark_domain::FindingStatus::Proposed
                        | threadmark_domain::FindingStatus::Accepted
                )
            })
            .count();
        Ok(EffortStatusView {
            effort,
            frontier,
            readiness,
            lint,
            active_fog,
            active_findings,
        })
    }

    pub async fn get_node(&self, effort: &str, node: &str) -> Result<Node, ApplicationError> {
        let effort = self.get_effort(effort).await?;
        Ok(self.store.get_node(&effort.id, node).await?)
    }

    pub async fn node_history(
        &self,
        effort: &str,
        node: &str,
    ) -> Result<Vec<NodeRevision>, ApplicationError> {
        let node = self.get_node(effort, node).await?;
        Ok(self.store.list_revisions(&node.id).await?)
    }

    pub async fn effort_history(&self, effort: &str) -> Result<Vec<AuditEvent>, ApplicationError> {
        self.history(EventFilter {
            effort_id: Some(effort.into()),
            ..EventFilter::default()
        })
        .await
    }

    pub async fn history(
        &self,
        mut filter: EventFilter,
    ) -> Result<Vec<AuditEvent>, ApplicationError> {
        if let Some(effort) = &filter.effort_id {
            let effort = self.get_effort(effort).await?;
            if effort.status == EffortStatus::Active {
                self.store.reap_expired_claims(&effort.id).await?;
            }
            filter.effort_id = Some(effort.id);
        } else {
            for effort in self.list_efforts().await? {
                if effort.status == EffortStatus::Active {
                    self.store.reap_expired_claims(&effort.id).await?;
                }
            }
        }
        Ok(self.store.list_events(&self.workspace.id, &filter).await?)
    }

    pub async fn claim_next(
        &self,
        effort: &str,
        claimant: &str,
        lease_minutes: i64,
    ) -> Result<Claim, ApplicationError> {
        let effort = self.active_effort(effort).await?;
        self.store.reap_expired_claims(&effort.id).await?;
        let graph = self.store.snapshot(&effort.id).await?;
        let timestamp = now();
        let frontier = calculate_frontier(&graph, &timestamp);
        let node = frontier
            .first()
            .ok_or_else(|| ApplicationError::NotOnFrontier("no ready nodes".into()))?;
        self.claim_node(&effort.slug, &node.node.id, claimant, lease_minutes)
            .await
    }

    pub async fn claim_node(
        &self,
        effort: &str,
        selector: &str,
        claimant: &str,
        lease_minutes: i64,
    ) -> Result<Claim, ApplicationError> {
        let effort = self.active_effort(effort).await?;
        self.store.reap_expired_claims(&effort.id).await?;
        let graph = self.store.snapshot(&effort.id).await?;
        let timestamp = now();
        let frontier = calculate_frontier(&graph, &timestamp);
        let entry = frontier
            .iter()
            .find(|entry| entry.node.id == selector || entry.node.id.starts_with(selector))
            .ok_or_else(|| ApplicationError::NotOnFrontier(selector.into()))?;
        let expires = (OffsetDateTime::now_utc() + Duration::minutes(lease_minutes.max(1)))
            .format(&Rfc3339)
            .expect("RFC3339 formatting succeeds");
        let claim = Claim {
            id: id(),
            node_id: entry.node.id.clone(),
            actor_id: claimant.into(),
            claimant: claimant.into(),
            claimed_at: timestamp.clone(),
            heartbeat_at: timestamp.clone(),
            lease_expires_at: expires,
            released_at: None,
            release_reason: None,
        };
        let audit = event(
            Some(&effort.id),
            claimant,
            None,
            "claim_acquired",
            "claim",
            &claim.id,
            None,
            Some(serde_json::to_value(&claim)?),
            None,
            &timestamp,
        );
        self.store.insert_claim(&claim, &audit).await?;
        Ok(claim)
    }

    pub async fn release_claim(
        &self,
        effort: &str,
        claim_id: &str,
        actor_id: &str,
        reason: &str,
    ) -> Result<(), ApplicationError> {
        let effort = self.get_effort(effort).await?;
        let timestamp = now();
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "claim_released",
            "claim",
            claim_id,
            None,
            None,
            Some(reason.into()),
            &timestamp,
        );
        self.store
            .release_claim(claim_id, actor_id, &timestamp, reason, &audit)
            .await?;
        Ok(())
    }

    pub async fn heartbeat_claim(
        &self,
        claim_id: &str,
        claimant: &str,
        lease_minutes: i64,
    ) -> Result<Claim, ApplicationError> {
        let heartbeat = now();
        let expires = (OffsetDateTime::now_utc() + Duration::minutes(lease_minutes.max(1)))
            .format(&Rfc3339)
            .expect("RFC3339 formatting succeeds");
        Ok(self
            .store
            .heartbeat_claim(claim_id, claimant, &heartbeat, &expires)
            .await?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_node(
        &self,
        effort: &str,
        selector: &str,
        actor_id: &str,
        session_id: Option<&str>,
        body: String,
        payload: Option<Value>,
        confidence: Option<Confidence>,
        confidence_reason: Option<String>,
        reason: &str,
        expected_version: Option<i64>,
    ) -> Result<(Node, i64), ApplicationError> {
        let node = self.get_node(effort, selector).await?;
        self.resolve_node_inner(
            effort,
            selector,
            actor_id,
            session_id,
            body,
            payload,
            confidence,
            confidence_reason,
            reason,
            expected_version,
            if node.claimable() {
                ClaimGuard::OwnIfClaimed(actor_id)
            } else {
                ClaimGuard::None
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_claimed_node(
        &self,
        effort: &str,
        selector: &str,
        claimant: &str,
        body: String,
        payload: Option<Value>,
        confidence: Option<Confidence>,
        confidence_reason: Option<String>,
        reason: &str,
        expected_version: Option<i64>,
    ) -> Result<(Node, i64), ApplicationError> {
        self.resolve_node_inner(
            effort,
            selector,
            claimant,
            None,
            body,
            payload,
            confidence,
            confidence_reason,
            reason,
            expected_version,
            ClaimGuard::MustOwn(claimant),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn resolve_harness_node(
        &self,
        effort: &str,
        selector: &str,
        claimant: &str,
        body: String,
        payload: Option<Value>,
        confidence: Option<Confidence>,
        confidence_reason: Option<String>,
        reason: &str,
        expected_version: Option<i64>,
    ) -> Result<(Node, i64), ApplicationError> {
        let node = self.get_node(effort, selector).await?;
        self.resolve_node_inner(
            effort,
            selector,
            claimant,
            None,
            body,
            payload,
            confidence,
            confidence_reason,
            reason,
            expected_version,
            if node.claimable() {
                ClaimGuard::MustOwn(claimant)
            } else {
                ClaimGuard::None
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_node_inner(
        &self,
        effort: &str,
        selector: &str,
        actor_id: &str,
        session_id: Option<&str>,
        body: String,
        payload: Option<Value>,
        confidence: Option<Confidence>,
        confidence_reason: Option<String>,
        reason: &str,
        expected_version: Option<i64>,
        claim_guard: ClaimGuard<'_>,
    ) -> Result<(Node, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let mut node = self.store.get_node(&effort.id, selector).await?;
        let before = serde_json::to_value(&node)?;
        node.lifecycle = Lifecycle::Resolved;
        node.validity = Validity::Current;
        node.body = body;
        if let Some(payload) = payload {
            node.payload = payload;
        }
        node.confidence = confidence.or(node.confidence);
        node.confidence_reason = confidence_reason.or(node.confidence_reason);
        node.current_revision += 1;
        node.updated_at = now();
        validate_node(&node)?;
        let mut revision = revision(&node, actor_id, session_id, reason, &node.updated_at);
        let mut audit = event(
            Some(&effort.id),
            actor_id,
            session_id,
            "node_resolved",
            "node",
            &node.id,
            Some(before),
            Some(serde_json::to_value(&node)?),
            Some(reason.into()),
            &node.updated_at,
        );
        let version = self
            .store
            .update_node(
                &mut node,
                Some(&mut revision),
                &mut audit,
                expected,
                claim_guard,
            )
            .await?;
        Ok((node, version))
    }

    pub async fn reopen_node(
        &self,
        effort: &str,
        selector: &str,
        actor_id: &str,
        reason: &str,
        expected_version: Option<i64>,
    ) -> Result<(Node, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let mut node = self.store.get_node(&effort.id, selector).await?;
        if node.lifecycle != Lifecycle::Resolved {
            return Err(
                DomainError::InvalidState("only resolved nodes can be reopened".into()).into(),
            );
        }
        let before = serde_json::to_value(&node)?;
        node.lifecycle = Lifecycle::Open;
        node.validity = Validity::ReviewRequired;
        node.current_revision += 1;
        node.updated_at = now();
        let mut revision = revision(&node, actor_id, None, reason, &node.updated_at);
        let mut audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "node_reopened",
            "node",
            &node.id,
            Some(before),
            Some(serde_json::to_value(&node)?),
            Some(reason.into()),
            &node.updated_at,
        );
        let version = self
            .store
            .update_node(
                &mut node,
                Some(&mut revision),
                &mut audit,
                expected,
                ClaimGuard::None,
            )
            .await?;
        Ok((node, version))
    }

    pub async fn invalidation_preview(
        &self,
        effort: &str,
        selector: &str,
        target: Validity,
    ) -> Result<InvalidationPreview, ApplicationError> {
        let effort = self.get_effort(effort).await?;
        let graph = self.store.snapshot(&effort.id).await?;
        let node = self.store.get_node(&effort.id, selector).await?;
        Ok(preview_invalidation(&graph, &node.id, target))
    }

    pub async fn commit_invalidation(
        &self,
        effort: &str,
        selector: &str,
        target: Validity,
        actor_id: &str,
        reason: &str,
        expected_version: Option<i64>,
    ) -> Result<(InvalidationPreview, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let graph = self.store.snapshot(&effort.id).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let node = self.store.get_node(&effort.id, selector).await?;
        let preview = preview_invalidation(&graph, &node.id, target);
        let root_node_id = node.id.clone();
        let timestamp = now();
        let nodes: HashMap<_, _> = graph
            .nodes
            .into_iter()
            .map(|node| (node.id.clone(), node))
            .collect();
        let mut changed = HashMap::new();
        for change in &preview.changes {
            let Some(mut node) = nodes.get(&change.node_id).cloned() else {
                continue;
            };
            if node.validity == change.to {
                continue;
            }
            node.validity = change.to;
            let revision_reason = if node.id == root_node_id {
                reason.to_owned()
            } else {
                change.reason.clone()
            };
            changed.insert(node.id.clone(), (node, revision_reason));
        }
        for question in &preview.reopened_questions {
            if !changed.contains_key(question) {
                let Some(node) = nodes.get(question).cloned() else {
                    continue;
                };
                changed.insert(
                    question.clone(),
                    (node, "all resolvers became unusable".into()),
                );
            }
            let Some((node, _)) = changed.get_mut(question) else {
                continue;
            };
            if node.lifecycle == Lifecycle::Open && node.validity == Validity::ReviewRequired {
                continue;
            }
            node.lifecycle = Lifecycle::Open;
            node.validity = Validity::ReviewRequired;
        }
        let updates = changed
            .into_values()
            .filter_map(|(mut node, reason)| {
                let original = nodes.get(&node.id)?;
                if node.lifecycle == original.lifecycle && node.validity == original.validity {
                    return None;
                }
                node.current_revision += 1;
                node.updated_at = timestamp.clone();
                let revision = revision(&node, actor_id, None, &reason, &timestamp);
                Some((node, revision))
            })
            .collect::<Vec<_>>();
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "invalidation_committed",
            "node",
            &node.id,
            None,
            Some(serde_json::to_value(&preview)?),
            Some(reason.into()),
            &timestamp,
        );
        let version = self
            .store
            .apply_invalidation(
                &effort.id,
                &updates,
                &preview.reopened_questions,
                &audit,
                expected,
                &timestamp,
            )
            .await?;
        Ok((preview, version))
    }

    pub async fn sources(
        &self,
        effort: &str,
    ) -> Result<Vec<threadmark_domain::Source>, ApplicationError> {
        let effort = self.get_effort(effort).await?;
        Ok(self.store.list_sources(&effort.id).await?)
    }

    pub async fn add_fog(
        &self,
        effort: &str,
        title: String,
        description: String,
        anchor: Option<String>,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<(FogPatch, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let timestamp = now();
        let anchor_node_id = match anchor {
            Some(selector) => Some(self.store.get_node(&effort.id, &selector).await?.id),
            None => None,
        };
        let fog = FogPatch {
            id: id(),
            effort_id: effort.id.clone(),
            title,
            description,
            anchor_node_id,
            status: FogStatus::Active,
            graduated_to: vec![],
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
        };
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "fog_created",
            "fog",
            &fog.id,
            None,
            Some(serde_json::to_value(&fog)?),
            None,
            &timestamp,
        );
        let version = self.store.insert_fog(&fog, &audit, expected).await?;
        Ok((fog, version))
    }

    pub async fn graduate_fog(
        &self,
        effort: &str,
        fog_id: &str,
        node_selectors: &[String],
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<i64, ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let mut node_ids = Vec::with_capacity(node_selectors.len());
        for selector in node_selectors {
            node_ids.push(self.store.get_node(&effort.id, selector).await?.id);
        }
        if node_ids.is_empty() {
            return Err(DomainError::InvalidState(
                "graduated fog requires at least one target node".into(),
            )
            .into());
        }
        let timestamp = now();
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "fog_graduated",
            "fog",
            fog_id,
            None,
            Some(json!({"nodes":node_ids})),
            None,
            &timestamp,
        );
        Ok(self
            .store
            .graduate_fog(&effort.id, fog_id, &node_ids, &audit, expected, &timestamp)
            .await?)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn add_source(
        &self,
        effort: &str,
        kind: SourceKind,
        title: String,
        uri: Option<String>,
        excerpt: Option<String>,
        trust: SourceTrust,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<(Source, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        if excerpt.as_deref().is_some_and(|value| value.len() > 4_096) {
            return Err(DomainError::InvalidState(
                "source excerpts are limited to 4096 bytes".into(),
            )
            .into());
        }
        let timestamp = now();
        let source = Source {
            id: id(),
            effort_id: effort.id.clone(),
            kind,
            uri,
            title,
            retrieved_at: Some(timestamp.clone()),
            observed_at: None,
            content_hash: None,
            excerpt,
            metadata: json!({}),
            trust,
            created_at: timestamp.clone(),
        };
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "source_created",
            "source",
            &source.id,
            None,
            Some(serde_json::to_value(&source)?),
            None,
            &timestamp,
        );
        let version = self.store.insert_source(&source, &audit, expected).await?;
        Ok((source, version))
    }

    pub async fn attach_source(
        &self,
        effort: &str,
        node_selector: &str,
        source_id: &str,
        relationship: &str,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<i64, ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let node = self.store.get_node(&effort.id, node_selector).await?;
        let timestamp = now();
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "source_attached",
            "node",
            &node.id,
            None,
            Some(json!({"source_id":source_id,"relationship":relationship})),
            None,
            &timestamp,
        );
        Ok(self
            .store
            .attach_source(
                &effort.id,
                &node.id,
                source_id,
                relationship,
                &audit,
                expected,
                &timestamp,
            )
            .await?)
    }

    pub async fn add_exit_criterion(
        &self,
        effort: &str,
        criterion_type: String,
        config: Value,
        required: bool,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<(ExitCriterion, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let supported = [
            "no_open_required_nodes",
            "no_active_fog",
            "no_undermined_decisions",
            "no_review_required_decisions",
            "no_blocking_findings",
            "requires_confidence_for_reversibility",
            "node_resolved",
            "node_valid",
        ];
        if !supported.contains(&criterion_type.as_str()) {
            return Err(DomainError::InvalidState(format!(
                "unsupported exit criterion: {criterion_type}"
            ))
            .into());
        }
        let timestamp = now();
        let criterion = ExitCriterion {
            id: id(),
            effort_id: effort.id.clone(),
            criterion_type,
            config,
            required,
            created_at: timestamp.clone(),
        };
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "exit_criterion_created",
            "exit_criterion",
            &criterion.id,
            None,
            Some(serde_json::to_value(&criterion)?),
            None,
            &timestamp,
        );
        let version = self
            .store
            .insert_criterion(&criterion, &audit, expected)
            .await?;
        Ok((criterion, version))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn propose_contradiction(
        &self,
        effort: &str,
        left: &str,
        right: &str,
        detail: String,
        severity: RiskLevel,
        actor_id: &str,
        expected_version: Option<i64>,
    ) -> Result<(Finding, i64), ApplicationError> {
        let effort = self.active_effort(effort).await?;
        let expected = expected_version.unwrap_or(effort.version);
        let left = self.store.get_node(&effort.id, left).await?;
        let right = self.store.get_node(&effort.id, right).await?;
        let timestamp = now();
        let finding = Finding {
            id: id(),
            effort_id: effort.id.clone(),
            finding_type: FindingType::Contradiction,
            severity,
            status: FindingStatus::Proposed,
            title: format!("Possible contradiction: {} / {}", left.title, right.title),
            detail,
            related_nodes: vec![left.id, right.id],
            proposed_by: Some(actor_id.into()),
            adjudication: None,
            created_at: timestamp.clone(),
            updated_at: timestamp.clone(),
        };
        let audit = event(
            Some(&effort.id),
            actor_id,
            None,
            "finding_proposed",
            "finding",
            &finding.id,
            None,
            Some(serde_json::to_value(&finding)?),
            None,
            &timestamp,
        );
        let version = self
            .store
            .insert_finding(&finding, &audit, expected)
            .await?;
        Ok((finding, version))
    }
}

fn validate_node(node: &Node) -> Result<(), ApplicationError> {
    if node.title.trim().is_empty() {
        return Err(DomainError::InvalidState("node title cannot be empty".into()).into());
    }
    if node.confidence.is_some() && node.confidence_reason.as_deref().is_none_or(str::is_empty) {
        return Err(DomainError::InvalidState("confidence requires a reason".into()).into());
    }
    if node.kind == threadmark_domain::NodeKind::Decision && node.lifecycle == Lifecycle::Resolved {
        let payload: threadmark_domain::DecisionPayload =
            serde_json::from_value(node.payload.clone())?;
        let selected = payload
            .alternatives
            .iter()
            .filter(|alternative| {
                alternative.status == threadmark_domain::AlternativeStatus::Selected
            })
            .count();
        if selected != 1 || payload.selected_option.is_none() {
            return Err(DomainError::InvalidState(
                "resolved decisions must select exactly one alternative".into(),
            )
            .into());
        }
    }
    Ok(())
}

fn revision(
    node: &Node,
    actor: &str,
    session: Option<&str>,
    reason: &str,
    timestamp: &str,
) -> NodeRevision {
    NodeRevision {
        node_id: node.id.clone(),
        revision: node.current_revision,
        body: node.body.clone(),
        payload: node.payload.clone(),
        reason: Some(reason.into()),
        actor_id: actor.into(),
        session_id: session.map(str::to_owned),
        created_at: timestamp.into(),
    }
}

#[allow(clippy::too_many_arguments)]
fn event(
    effort_id: Option<&str>,
    actor: &str,
    session: Option<&str>,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    before: Option<Value>,
    after: Option<Value>,
    reason: Option<String>,
    timestamp: &str,
) -> AuditEvent {
    AuditEvent {
        id: id(),
        effort_id: effort_id.map(str::to_owned),
        actor_id: actor.into(),
        session_id: session.map(str::to_owned),
        event_type: event_type.into(),
        entity_type: entity_type.into(),
        entity_id: entity_id.into(),
        before,
        after,
        reason,
        occurred_at: timestamp.into(),
    }
}

fn id() -> String {
    Ulid::new().to_string()
}

fn now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("RFC3339 formatting succeeds")
}

fn discover_root(start: &Path) -> Result<PathBuf, ApplicationError> {
    let mut current = start.canonicalize().unwrap_or_else(|_| start.to_path_buf());
    loop {
        if current.join(MARKER).exists() {
            return Ok(current);
        }
        if !current.pop() {
            return Err(ApplicationError::NotInitialized(
                start.display().to_string(),
            ));
        }
    }
}

fn database_path(root: &Path) -> PathBuf {
    let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-common-dir"])
        .current_dir(root)
        .output()
    else {
        return root.join(".threadmark/state.sqlite3");
    };
    if !output.status.success() {
        return root.join(".threadmark/state.sqlite3");
    }
    let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    let common = if path.is_absolute() {
        path
    } else {
        root.join(path)
    };
    common.join("threadmark/state.sqlite3")
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;
    use threadmark_domain::{EdgeType, NodeKind, RiskLevel, Uncertainty};

    use super::*;

    async fn add_test_node(service: &Service, effort: &str, kind: NodeKind, title: &str) -> Node {
        service
            .add_node(AddNode {
                effort: effort.into(),
                node: NewNode {
                    kind,
                    title: title.into(),
                    summary: String::new(),
                    body: String::new(),
                    payload: json!({}),
                    lifecycle: Lifecycle::Open,
                    confidence: None,
                    confidence_reason: None,
                    reversibility: None,
                    impact: None,
                    uncertainty: None,
                    cost_of_wrong: None,
                },
                actor_id: "test".into(),
                session_id: None,
                expected_version: None,
            })
            .await
            .unwrap()
            .0
    }

    async fn insert_expired_claim(service: &Service, effort: &Effort, node: &Node) {
        let timestamp = now();
        let claim = Claim {
            id: id(),
            node_id: node.id.clone(),
            actor_id: "test".into(),
            claimant: "test".into(),
            claimed_at: timestamp.clone(),
            heartbeat_at: timestamp.clone(),
            lease_expires_at: "2020-01-01T00:00:00Z".into(),
            released_at: None,
            release_reason: None,
        };
        let audit = event(
            Some(&effort.id),
            "test",
            None,
            "claim_acquired",
            "claim",
            &claim.id,
            None,
            None,
            None,
            &timestamp,
        );
        service.store.insert_claim(&claim, &audit).await.unwrap();
    }

    #[tokio::test]
    async fn creates_effort_and_frontier_node() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "cache".into(),
                title: "Cache".into(),
                destination: "Choose cache architecture".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        service
            .add_node(AddNode {
                effort: effort.slug.clone(),
                node: NewNode {
                    kind: NodeKind::Question,
                    title: "What workload?".into(),
                    summary: String::new(),
                    body: String::new(),
                    payload: json!({}),
                    lifecycle: Lifecycle::Open,
                    confidence: None,
                    confidence_reason: None,
                    reversibility: None,
                    impact: Some(RiskLevel::High),
                    uncertainty: Some(Uncertainty::High),
                    cost_of_wrong: Some(RiskLevel::High),
                },
                actor_id: "test".into(),
                session_id: None,
                expected_version: None,
            })
            .await
            .unwrap();
        let status = service.status("cache").await.unwrap();
        assert_eq!(status.frontier.len(), 1);
        assert!(!status.readiness.ready);
    }

    #[tokio::test]
    async fn filters_history_without_session_data() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "history".into(),
                title: "History".into(),
                destination: "Test history filters".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let node = add_test_node(&service, &effort.slug, NodeKind::Action, "recorded").await;

        let events = service
            .history(EventFilter {
                effort_id: Some(effort.slug),
                entity_type: Some("node".into()),
                entity_id: Some(node.id),
                actor_id: Some("test".into()),
                event_type: Some("node_created".into()),
                occurred_from: Some(node.created_at.clone()),
                occurred_to: Some(node.created_at),
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert!(
            serde_json::to_value(&events[0])
                .unwrap()
                .get("session_id")
                .is_none()
        );
    }

    #[tokio::test]
    async fn snapshot_reaps_expired_claims_before_reading_the_graph() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "export".into(),
                title: "Export".into(),
                destination: "Test snapshot consistency".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let node = add_test_node(&service, &effort.slug, NodeKind::Action, "expired").await;
        insert_expired_claim(&service, &effort, &node).await;

        let (_, graph, _, events) = service.export_snapshot(&effort.slug).await.unwrap();

        assert_eq!(graph.nodes[0].lifecycle, Lifecycle::Open);
        assert!(
            events
                .iter()
                .any(|event| event.event_type == "claim_expired")
        );
    }

    #[tokio::test]
    async fn resolving_after_a_lease_expiry_records_the_expiration() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "resolve".into(),
                title: "Resolve".into(),
                destination: "Test expiration before resolution".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let node = add_test_node(&service, &effort.slug, NodeKind::Action, "expired").await;
        insert_expired_claim(&service, &effort, &node).await;

        service
            .resolve_node(
                &effort.slug,
                &node.id,
                "test",
                None,
                "resolved".into(),
                None,
                None,
                None,
                "resolved",
                None,
            )
            .await
            .unwrap();

        assert!(
            service
                .effort_history(&effort.slug)
                .await
                .unwrap()
                .iter()
                .any(|event| event.event_type == "claim_expired")
        );
    }

    #[tokio::test]
    async fn completes_a_ready_effort_and_records_an_event() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "complete".into(),
                title: "Complete".into(),
                destination: "Finish".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();

        let completed = service
            .complete_effort(&effort.slug, "test", Some(effort.version))
            .await
            .unwrap();
        assert_eq!(completed.status, EffortStatus::Completed);
        assert_eq!(completed.version, effort.version + 1);
        assert!(
            service
                .effort_history(&effort.slug)
                .await
                .unwrap()
                .iter()
                .any(|event| event.event_type == "effort_completed")
        );
        assert!(matches!(
            service
                .add_node(AddNode {
                    effort: effort.slug,
                    node: NewNode {
                        kind: NodeKind::Question,
                        title: "Late question".into(),
                        summary: String::new(),
                        body: String::new(),
                        payload: json!({}),
                        lifecycle: Lifecycle::Open,
                        confidence: None,
                        confidence_reason: None,
                        reversibility: None,
                        impact: None,
                        uncertainty: None,
                        cost_of_wrong: None,
                    },
                    actor_id: "test".into(),
                    session_id: None,
                    expected_version: None,
                })
                .await,
            Err(ApplicationError::EffortNotActive(EffortStatus::Completed))
        ));
    }

    #[tokio::test]
    async fn rejects_completion_when_readiness_fails() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "not-ready".into(),
                title: "Not ready".into(),
                destination: "Finish".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        service
            .add_node(AddNode {
                effort: effort.slug.clone(),
                node: NewNode {
                    kind: NodeKind::Question,
                    title: "Unresolved".into(),
                    summary: String::new(),
                    body: String::new(),
                    payload: json!({}),
                    lifecycle: Lifecycle::Open,
                    confidence: None,
                    confidence_reason: None,
                    reversibility: None,
                    impact: Some(RiskLevel::High),
                    uncertainty: Some(Uncertainty::High),
                    cost_of_wrong: Some(RiskLevel::High),
                },
                actor_id: "test".into(),
                session_id: None,
                expected_version: None,
            })
            .await
            .unwrap();

        assert!(matches!(
            service.complete_effort(&effort.slug, "test", None).await,
            Err(ApplicationError::EffortNotReady(_))
        ));
    }

    #[tokio::test]
    async fn rejects_completion_with_an_active_claim() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "claimed".into(),
                title: "Claimed".into(),
                destination: "Finish".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let (node, _) = service
            .add_node(AddNode {
                effort: effort.slug.clone(),
                node: NewNode {
                    kind: NodeKind::Question,
                    title: "Claimed question".into(),
                    summary: String::new(),
                    body: String::new(),
                    payload: json!({}),
                    lifecycle: Lifecycle::Open,
                    confidence: None,
                    confidence_reason: None,
                    reversibility: None,
                    impact: None,
                    uncertainty: None,
                    cost_of_wrong: None,
                },
                actor_id: "test".into(),
                session_id: None,
                expected_version: None,
            })
            .await
            .unwrap();
        service
            .add_exit_criterion(
                &effort.slug,
                "no_active_fog".into(),
                json!({}),
                true,
                "test",
                None,
            )
            .await
            .unwrap();
        service
            .claim_node(&effort.slug, &node.id, "test", 30)
            .await
            .unwrap();

        assert!(matches!(
            service.complete_effort(&effort.slug, "test", None).await,
            Err(ApplicationError::Store(StoreError::ActiveClaims))
        ));
    }

    #[tokio::test]
    async fn resolves_an_unclaimed_in_progress_node() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "in-progress".into(),
                title: "In progress".into(),
                destination: "Finish".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let (node, _) = service
            .add_node(AddNode {
                effort: effort.slug.clone(),
                node: NewNode {
                    kind: NodeKind::Action,
                    title: "Already in progress".into(),
                    summary: String::new(),
                    body: String::new(),
                    payload: json!({}),
                    lifecycle: Lifecycle::InProgress,
                    confidence: None,
                    confidence_reason: None,
                    reversibility: None,
                    impact: None,
                    uncertainty: None,
                    cost_of_wrong: None,
                },
                actor_id: "test".into(),
                session_id: None,
                expected_version: None,
            })
            .await
            .unwrap();

        let (resolved, _) = service
            .resolve_node(
                &effort.slug,
                &node.id,
                "test",
                None,
                "done".into(),
                None,
                None,
                None,
                "done",
                None,
            )
            .await
            .unwrap();
        assert_eq!(resolved.lifecycle, Lifecycle::Resolved);
    }

    #[tokio::test]
    async fn claim_mutations_require_the_active_harness_claimant() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "claim-owner".into(),
                title: "Claim owner".into(),
                destination: "Test claim ownership".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let node = add_test_node(&service, &effort.slug, NodeKind::Action, "owned").await;
        let claim = service
            .claim_node(&effort.slug, &node.id, "openai-codex", 120)
            .await
            .unwrap();

        assert!(matches!(
            service
                .release_claim(&effort.slug, &claim.id, "claude-code", "released")
                .await,
            Err(ApplicationError::Store(StoreError::ClaimNotOwned(_)))
        ));
        assert!(matches!(
            service.heartbeat_claim(&claim.id, "claude-code", 30).await,
            Err(ApplicationError::Store(StoreError::ClaimNotOwned(_)))
        ));
        assert!(matches!(
            service
                .resolve_node(
                    &effort.slug,
                    &node.id,
                    "claude-code",
                    None,
                    "resolved".into(),
                    None,
                    None,
                    None,
                    "resolved",
                    None,
                )
                .await,
            Err(ApplicationError::Store(StoreError::ClaimNotOwned(_)))
        ));
        let heartbeat = service
            .heartbeat_claim(&claim.id, "openai-codex", 30)
            .await
            .unwrap();
        assert_eq!(heartbeat.lease_expires_at, claim.lease_expires_at);
        assert!(matches!(
            service
                .resolve_claimed_node(
                    &effort.slug,
                    &node.id,
                    "claude-code",
                    "resolved".into(),
                    None,
                    None,
                    None,
                    "resolved",
                    None,
                )
                .await,
            Err(ApplicationError::Store(StoreError::ClaimNotOwned(_)))
        ));

        service
            .resolve_claimed_node(
                &effort.slug,
                &node.id,
                "openai-codex",
                "resolved".into(),
                None,
                None,
                None,
                "resolved",
                None,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn concurrent_claims_have_exactly_one_winner() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "claim-race".into(),
                title: "Claim race".into(),
                destination: "Test claim exclusivity".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let node = add_test_node(&service, &effort.slug, NodeKind::Action, "contended").await;

        let (first, second) = tokio::join!(
            service.claim_node(&effort.slug, &node.id, "first", 30),
            service.claim_node(&effort.slug, &node.id, "second", 30),
        );

        assert_eq!(
            [first.is_ok(), second.is_ok()]
                .into_iter()
                .filter(|won| *won)
                .count(),
            1
        );
        assert_eq!(
            service.snapshot(&effort.slug).await.unwrap().1.claims.len(),
            1
        );
    }

    #[tokio::test]
    async fn non_claimable_nodes_resolve_without_a_claim() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "evidence".into(),
                title: "Evidence".into(),
                destination: "Test evidence resolution".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let evidence = add_test_node(&service, &effort.slug, NodeKind::Evidence, "evidence").await;

        service
            .resolve_harness_node(
                &effort.slug,
                &evidence.id,
                "openai-codex",
                "recorded".into(),
                None,
                None,
                None,
                "recorded",
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            service
                .get_node(&effort.slug, &evidence.id)
                .await
                .unwrap()
                .lifecycle,
            Lifecycle::Resolved
        );
    }

    #[tokio::test]
    async fn opens_a_marker_without_local_database_and_marker_metadata_wins() {
        let directory = TempDir::new().unwrap();
        let marker_directory = directory.path().join(".threadmark");
        std::fs::create_dir_all(&marker_directory).unwrap();
        std::fs::write(marker_directory.join("workspace.toml"), "schema_version = 1\nworkspace_id = \"01TESTWORKSPACE000000000000\"\nname = \"committed\"\n").unwrap();

        let service = Service::open(directory.path()).await.unwrap();
        assert_eq!(service.workspace().name, "committed");

        std::fs::write(marker_directory.join("workspace.toml"), "schema_version = 1\nworkspace_id = \"01TESTWORKSPACE000000000000\"\nname = \"renamed\"\n").unwrap();
        let service = Service::open(directory.path()).await.unwrap();
        assert_eq!(service.workspace().name, "renamed");
    }

    #[tokio::test]
    async fn rejects_a_newer_marker_schema() {
        let directory = TempDir::new().unwrap();
        let marker_directory = directory.path().join(".threadmark");
        std::fs::create_dir_all(&marker_directory).unwrap();
        std::fs::write(marker_directory.join("workspace.toml"), "schema_version = 2\nworkspace_id = \"01TESTWORKSPACE000000000000\"\nname = \"future\"\n").unwrap();

        assert!(matches!(
            Service::open(directory.path()).await,
            Err(ApplicationError::InvalidMarker(_))
        ));
    }

    #[tokio::test]
    async fn invalidation_preserves_revisions_and_only_reviews_resolved_requirements() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let effort = service
            .create_effort(CreateEffort {
                slug: "invalidation".into(),
                title: "Invalidation".into(),
                destination: "Test invalidation".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let assumption =
            add_test_node(&service, &effort.slug, NodeKind::Assumption, "premise").await;
        let direct = add_test_node(&service, &effort.slug, NodeKind::Action, "direct").await;
        let required = add_test_node(&service, &effort.slug, NodeKind::Action, "required").await;
        let question = add_test_node(&service, &effort.slug, NodeKind::Question, "question").await;
        let open = add_test_node(&service, &effort.slug, NodeKind::Action, "open").await;

        for (source, edge_type, target) in [
            (&direct, EdgeType::Assumes, &assumption),
            (&required, EdgeType::Requires, &direct),
            (&open, EdgeType::Requires, &direct),
            (&direct, EdgeType::Resolves, &question),
        ] {
            service
                .add_edge(AddEdge {
                    effort: effort.slug.clone(),
                    edge: NewEdge {
                        source_node_id: source.id.clone(),
                        edge_type,
                        target_node_id: target.id.clone(),
                        rationale: None,
                    },
                    actor_id: "test".into(),
                    expected_version: None,
                })
                .await
                .unwrap();
        }
        for node in [&assumption, &direct, &required, &question] {
            if node.claimable() {
                service
                    .claim_node(&effort.slug, &node.id, "test", 30)
                    .await
                    .unwrap();
            }
            service
                .resolve_node(
                    &effort.slug,
                    &node.id,
                    "test",
                    None,
                    "resolved".into(),
                    None,
                    None,
                    None,
                    "test setup",
                    None,
                )
                .await
                .unwrap();
        }

        let current = service.snapshot(&effort.slug).await.unwrap().0;
        let preview = service
            .invalidation_preview(&effort.slug, &assumption.id, Validity::Invalid)
            .await
            .unwrap();
        assert!(matches!(
            service
                .commit_invalidation(
                    &effort.slug,
                    &assumption.id,
                    Validity::Invalid,
                    "test",
                    "stale",
                    Some(current.version - 1),
                )
                .await,
            Err(ApplicationError::Store(StoreError::VersionConflict { .. }))
        ));
        assert_eq!(
            service
                .node_history(&effort.slug, &assumption.id)
                .await
                .unwrap()
                .len(),
            2
        );

        let (committed, version) = service
            .commit_invalidation(
                &effort.slug,
                &assumption.id,
                Validity::Invalid,
                "test",
                "premise disproven",
                Some(current.version),
            )
            .await
            .unwrap();
        assert_eq!(committed, preview);
        assert_eq!(version, current.version + 1);
        assert_eq!(
            service
                .node_history(&effort.slug, &assumption.id)
                .await
                .unwrap()
                .last()
                .unwrap()
                .reason
                .as_deref(),
            Some("premise disproven")
        );

        assert_eq!(
            service
                .get_node(&effort.slug, &assumption.id)
                .await
                .unwrap()
                .validity,
            Validity::Invalid
        );
        assert_eq!(
            service
                .get_node(&effort.slug, &direct.id)
                .await
                .unwrap()
                .validity,
            Validity::Undermined
        );
        assert_eq!(
            service
                .get_node(&effort.slug, &required.id)
                .await
                .unwrap()
                .validity,
            Validity::ReviewRequired
        );
        assert_eq!(
            service
                .get_node(&effort.slug, &open.id)
                .await
                .unwrap()
                .validity,
            Validity::Current
        );
        let reopened = service.get_node(&effort.slug, &question.id).await.unwrap();
        assert_eq!(reopened.lifecycle, Lifecycle::Open);
        assert_eq!(reopened.validity, Validity::ReviewRequired);
        for node in [&assumption, &direct, &required, &question] {
            assert_eq!(
                service
                    .node_history(&effort.slug, &node.id)
                    .await
                    .unwrap()
                    .len(),
                3
            );
        }
    }

    #[tokio::test]
    async fn preserves_escaped_workspace_names() {
        let directory = TempDir::new().unwrap();
        let name = "quoted \"name\" with \\ slash\nand newline";
        Service::init(directory.path(), name).await.unwrap();

        let service = Service::open(directory.path()).await.unwrap();
        assert_eq!(service.workspace().name, name);
    }

    #[tokio::test]
    async fn preserves_workspace_creation_time_when_reopened() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let created_at = service.workspace().created_at.clone();

        let service = Service::open(directory.path()).await.unwrap();
        assert_eq!(service.workspace().created_at, created_at);
    }

    #[tokio::test]
    async fn preserves_workspace_updated_time_when_reopened_unchanged() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let updated_at = service.workspace().updated_at.clone();

        let service = Service::open(directory.path()).await.unwrap();
        assert_eq!(service.workspace().updated_at, updated_at);
    }

    #[tokio::test]
    async fn migrates_efforts_when_the_marker_replaces_a_workspace_id() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let old_id = service.workspace().id.clone();
        service
            .create_effort(CreateEffort {
                slug: "effort".into(),
                title: "Effort".into(),
                destination: "Destination".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        std::fs::write(
            directory.path().join(MARKER),
            "schema_version = 1\nworkspace_id = \"01TESTWORKSPACE000000000000\"\nname = \"test\"\n",
        )
        .unwrap();

        let service = Service::open(directory.path()).await.unwrap();
        assert_ne!(service.workspace().id, old_id);
        assert_eq!(service.list_efforts().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn migrates_a_root_matched_workspace_when_the_marker_id_already_exists() {
        let directory = TempDir::new().unwrap();
        let service = Service::init(directory.path(), "test").await.unwrap();
        let legacy_effort = service
            .create_effort(CreateEffort {
                slug: "effort".into(),
                title: "Effort".into(),
                destination: "Destination".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        service
            .create_effort(CreateEffort {
                slug: format!("effort-{}", legacy_effort.id),
                title: "Existing suffix".into(),
                destination: "Destination".into(),
                scope_notes: String::new(),
                actor_id: "test".into(),
            })
            .await
            .unwrap();
        let timestamp = now();
        service
            .store
            .create_workspace(&Workspace {
                id: "01TESTWORKSPACE000000000000".into(),
                name: "test".into(),
                root_uri: "other-worktree".into(),
                schema_version: SCHEMA_VERSION,
                created_at: timestamp.clone(),
                updated_at: timestamp.clone(),
            })
            .await
            .unwrap();
        service
            .store
            .create_effort(
                &Effort {
                    id: "01TARGETEFFORT00000000000000".into(),
                    workspace_id: "01TESTWORKSPACE000000000000".into(),
                    slug: "effort".into(),
                    title: "Target effort".into(),
                    destination: "Destination".into(),
                    scope_notes: String::new(),
                    status: EffortStatus::Active,
                    version: 1,
                    created_at: timestamp.clone(),
                    updated_at: timestamp.clone(),
                },
                &event(
                    None,
                    "test",
                    None,
                    "effort_created",
                    "effort",
                    "01TARGETEFFORT00000000000000",
                    None,
                    None,
                    None,
                    &timestamp,
                ),
            )
            .await
            .unwrap();
        std::fs::write(
            directory.path().join(MARKER),
            "schema_version = 1\nworkspace_id = \"01TESTWORKSPACE000000000000\"\nname = \"test\"\n",
        )
        .unwrap();

        let service = Service::open(directory.path()).await.unwrap();
        let efforts = service.list_efforts().await.unwrap();
        assert_eq!(efforts.len(), 3);
        assert!(
            efforts
                .iter()
                .any(|effort| effort.slug == format!("effort-{}-2", legacy_effort.id))
        );
    }
}
